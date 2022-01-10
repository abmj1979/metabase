(ns metabase.query-processor.util.nest-query
  "Utility functions for raising/nesting parts of MBQL queries. Currently, this only has [[nest-expressions]], but in
  the future hopefully we can generalize this a bit so we can do more things that require us to introduce another
  level of nesting, e.g. support window functions.

   (This namespace is here rather than in the shared MBQL lib because it relies on other QP-land utils like the QP
  refs stuff.)"
  (:require [metabase.mbql.util :as mbql.u]
            [metabase.plugins.classloader :as classloader]
            [metabase.query-processor.error-type :as qp.error-type]
            [metabase.query-processor.middleware.annotate :as annotate]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.util.add-alias-info :as add]
            [metabase.util :as u]
            [metabase.util.i18n :refer [tru]]))

(defn- joined-fields [inner-query]
  (into #{} (mbql.u/match (dissoc inner-query :source-query :source-metadata)
              [:field _ (_ :guard :join-alias)]
              &match)))

(defn- add-joined-fields-to-fields [joined-fields source]
  (cond-> source
    (seq joined-fields) (update :fields (fn [fields]
                                          (distinct (concat fields joined-fields))))))

(defn- nest-source [inner-query]
  (classloader/require 'metabase.query-processor)
  (let [source (as-> (select-keys inner-query [:source-table :source-query :source-metadata :joins :expressions]) source
                 ((resolve 'metabase.query-processor/query->preprocessed) {:database (u/the-id (qp.store/database))
                                                                           :type     :query
                                                                           :query    source})
                 (:query source)
                 (dissoc source :limit)
                 (add-joined-fields-to-fields (joined-fields inner-query) source))]
    (-> inner-query
        (dissoc :source-table :source-metadata :joins)
        (assoc :source-query source))))

(defn- raise-source-query-expression-ref
  "Convert an `:expression` reference from a source query into an appropriate `:field` clause for use in the surrounding
  query."
  [{:keys [expressions], :as source-query} [_ expression-name opts]]
  (let [expression-definition (or (get expressions (keyword expression-name))
                                  (throw (ex-info (tru "No expression named {0}" (pr-str expression-name))
                                                  {:type            qp.error-type/invalid-query
                                                   :expression-name expression-name
                                                   :query           source-query})))
        {base-type :base_type} (some-> expression-definition annotate/infer-expression-type)]
    [:field expression-name (assoc opts :base-type (or base-type :type/*))]))

(defn- rewrite-fields-and-expressions [query]
  (mbql.u/replace query
    :expression
    (raise-source-query-expression-ref query &match)

    ;; mark all Fields at the new top level as `::outer-select` so QP implementations know not to apply coercion or
    ;; whatever to them a second time.
    [:field _id-or-name (_opts :guard :temporal-unit)]
    (mbql.u/update-field-options &match assoc ::outer-select true)

    ;; [:field id-or-name (opts :guard :join-alias)]
    ;; (let [{field-alias :alias} (refs/field-ref-info query &match)]
    ;;   (assert field-alias)
    ;;   [:field field-alias {:base-type :type/Integer}])

    ;; when recursing into joins use the refs from the parent level.
    (m :guard (every-pred map? :joins))
    (let [{:keys [joins]} m]
      (-> (dissoc m :joins)
          rewrite-fields-and-expressions
          (assoc :joins (mapv (fn [join]
                                (assoc join :qp/refs (:qp/refs query)))
                              joins))))

    ;; don't recurse into any `:source-query` maps.
    (m :guard (every-pred map? :source-query))
    (let [{:keys [source-query]} m]
      (-> (dissoc m :source-query)
          rewrite-fields-and-expressions
          (assoc :source-query source-query)))))

(defn nest-expressions
  "Pushes the `:source-table`/`:source-query`, `:expressions`, and `:joins` in the top-level of the query into a
  `:source-query` and updates `:expression` references and `:field` clauses with `:join-alias`es accordingly. See
  tests for examples. This is used by the SQL QP to make sure expressions happen in a subselect."
  [{:keys [expressions], :as query}]
  (if (empty? expressions)
    query
    (let [query                             (rewrite-fields-and-expressions query)
          {:keys [source-query], :as query} (nest-source query)
          source-query                      (assoc source-query :expressions expressions)]
      (-> query
          (dissoc :source-query :expressions)
          (assoc :source-query source-query)
          add/add-alias-info))))
