(ns metabase.db.metadata-queries
  "Predefined MBQL queries for getting metadata about an external database.

  TODO -- these have nothing to do with the application database. This namespace should be renamed something like
  `metabase.driver.util.metadata-queries`."
  (:require
   [metabase.driver :as driver]
   [metabase.driver.util :as driver.u]
   [metabase.mbql.schema :as mbql.s]
   [metabase.mbql.schema.helpers :as helpers]
   [metabase.models.table :as table :refer [Table]]
   [metabase.query-processor :as qp]
   [metabase.query-processor.interface :as qp.i]
   [metabase.sync.interface :as i]
   [metabase.util :as u]
   [metabase.util.malli :as mu]
   [metabase.util.malli.schema :as ms]
   [schema.core :as s]
   [toucan2.core :as t2]))

(defn- qp-query [db-id mbql-query]
  {:pre [(integer? db-id)]}
  (-> (binding [qp.i/*disable-qp-logging* true]
        (qp/process-query
         {:type       :query
          :database   db-id
          :query      mbql-query
          :middleware {:disable-remaps? true}}))
      :data
      :rows))

(defn- field-query [{table-id :table_id} mbql-query]
  {:pre [(integer? table-id)]}
  (qp-query (t2/select-one-fn :db_id Table, :id table-id)
            ;; this seeming useless `merge` statement IS in fact doing something important. `ql/query` is a threading
            ;; macro for building queries. Do not remove
            (assoc mbql-query :source-table table-id)))

(def ^Integer absolute-max-distinct-values-limit
  "The absolute maximum number of results to return for a `field-distinct-values` query. Normally Fields with 100 or
  less values (at the time of this writing) get marked as `auto-list` Fields, meaning we save all their distinct
  values in a FieldValues object, which powers a list widget in the FE when using the Field for filtering in the QB.
  Admins can however manually mark any Field as `list`, which is effectively ordering Metabase to keep FieldValues for
  the Field regardless of its cardinality.

  Of course, if a User does something crazy, like mark a million-arity Field as List, we don't want Metabase to
  explode trying to make their dreams a reality; we need some sort of hard limit to prevent catastrophes. So this
  limit is effectively a safety to prevent Users from nuking their own instance for Fields that really shouldn't be
  List Fields at all. For these very-high-cardinality Fields, we're effectively capping the number of
  FieldValues that get could saved.

  This number should be a balance of:

  * Not being too low, which would definitely result in GitHub issues along the lines of 'My 500-distinct-value Field
    that I marked as List is not showing all values in the List Widget'
  * Not being too high, which would result in Metabase running out of memory dealing with too many values"
  (int 1000))

(mu/defn field-distinct-values :- [:sequential ms/NonRemappedFieldValue]
  "Return the distinct values of `field`, each wrapped in a vector.
   This is used to create a `FieldValues` object for `:type/Category` Fields."
  ([field]
   (field-distinct-values field absolute-max-distinct-values-limit))

  ([field max-results :- ms/PositiveInt]
   (field-query field {:breakout [[:field (u/the-id field) nil]]
                       :limit    (min max-results absolute-max-distinct-values-limit)})))

(defn field-distinct-count
  "Return the distinct count of `field`."
  [field & [limit]]
  (-> (field-query field {:aggregation [[:distinct [:field (u/the-id field) nil]]]
                          :limit       limit})
      first first int))

(defn field-count
  "Return the count of `field`."
  [field]
  (-> (field-query field {:aggregation [[:count [:field (u/the-id field) nil]]]})
      first first int))

(def max-sample-rows
  "The maximum number of values we should return when using `table-rows-sample`. This many is probably fine for
  inferring semantic types and what-not; we don't want to scan millions of values at any rate."
  10000)

(def nested-field-sample-limit
  "Number of rows to sample for tables with nested (e.g., JSON) columns."
  500)

(def TableRowsSampleOptions
  "Schema for `table-rows-sample` options"
  (s/maybe {(s/optional-key :truncation-size)  s/Int
            (s/optional-key :limit)            s/Int
            (s/optional-key :order-by)         (helpers/distinct (helpers/non-empty [mbql.s/OrderBy]))
            (s/optional-key :rff)              s/Any}))

(defn- text-field?
  "Identify text fields which can accept our substring optimization.

  JSON and XML fields are now marked as `:type/Structured` but in the past were marked as `:type/Text` so its not
  enough to just check the base type."
  [{:keys [base_type semantic_type]}]
  (and (= base_type :type/Text)
       (not (isa? semantic_type :type/Structured))))

(defn- table-rows-sample-query
  "Returns the mbql query to query a table for sample rows"
  [table
   fields
   {:keys [truncation-size limit order-by] :or {limit max-sample-rows} :as _opts}]
  (let [database           (table/database table)
        driver             (driver.u/database->driver database)
        text-fields        (filter text-field? fields)
        field->expressions (when (and truncation-size (driver/database-supports? driver :expressions database))
                             (into {} (for [field text-fields]
                                        [field [(str (gensym "substring"))
                                                [:substring [:field (u/the-id field) nil]
                                                 1 truncation-size]]])))]
    {:database   (:db_id table)
     :type       :query
     :query      (cond-> {:source-table (u/the-id table)
                          :expressions  (into {} (vals field->expressions))
                          :fields       (vec (for [field fields]
                                               (if-let [[expression-name _] (get field->expressions field)]
                                                 [:expression expression-name]
                                                 [:field (u/the-id field) nil])))
                          :limit        limit}
                   order-by (assoc :order-by order-by))
     :middleware {:format-rows?           false
                  :skip-results-metadata? true}}))

(s/defn table-rows-sample
  "Run a basic MBQL query to fetch a sample of rows of FIELDS belonging to a TABLE.

  Options: a map of
  `:truncation-size`: [optional] size to truncate text fields if the driver supports expressions.
  `:rff`: [optional] a reducing function function (a function that given initial results metadata returns a reducing
  function) to reduce over the result set in the the query-processor rather than realizing the whole collection"
  {:style/indent 1}
  ([table :- i/TableInstance, fields :- [i/FieldInstance], rff]
   (table-rows-sample table fields rff nil))
  ([table :- i/TableInstance, fields :- [i/FieldInstance], rff, opts :- TableRowsSampleOptions]
   (let [query (table-rows-sample-query table fields opts)
         qp    (resolve 'metabase.query-processor/process-query)]
     (qp query {:rff rff}))))

(defmethod driver/table-rows-sample :default
  [_driver table fields rff opts]
  (table-rows-sample table fields rff opts))
