(ns metabase.query-processor.util.add-alias-info
  (:require [clojure.walk :as walk]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.error-type :as qp.error-type]
            [metabase.query-processor.store :as qp.store]
            [metabase.util.i18n :refer [tru]]
            [metabase.util :as u]))

(defn- remove-namespaced-options [options]
  (not-empty (into {}
                   (remove (fn [[k _]]
                             (when (keyword? k)
                               (namespace k))))
                   options)))

#_(defn- remove-default-temporal-unit [{:keys [temporal-unit], :as options}]
  (cond-> options
    (= temporal-unit :default)
    (dissoc :temporal-unit)))

(defn normalize-clause
  "Normalize a `:field`/`:expression`/`:aggregation` clause by removing extra info so it can serve as a key for
  `:qp/refs`."
  [clause]
  (mbql.u/match-one clause
    :field
    (mbql.u/update-field-options &match (comp remove-namespaced-options
                                              #_remove-default-temporal-unit
                                              #(dissoc % :source-field)))

    [:expression expression-name opts]
    [:expression expression-name (remove-namespaced-options opts)]

    [:aggregation index opts]
    [:aggregation index (remove-namespaced-options opts)]

    :else
    &match))

(defn- clause-options [clause]
  (mbql.u/match-one clause
    [:field _ opts]       opts
    [:expression _ opts]  opts
    [:aggregation _ opts] opts))

(defn- selected-clauses [{:keys [fields breakout aggregation]}]
  (into
   {}
   (comp cat
         (map-indexed
          (fn [i clause]
            [(normalize-clause clause) i])))
   [breakout
    (map-indexed
     (fn [i ag]
       (mbql.u/replace ag
         [:aggregation-options wrapped opts]
         [:aggregation i]

         ;; aggregation clause should be preprocessed into an `:aggregation-options` clause by now.
         _
         (throw (ex-info (tru "Expected :aggregation-options clause, got {0}" (pr-str ag))
                         {:type qp.error-type/qp, :clause ag}))))
     aggregation)
    fields]))

(defn- exports [query]
  (into #{} (mbql.u/match (dissoc query :source-query :source-metadata)
              [(_ :guard #{:field :expression :aggregation}) _ (_ :guard (every-pred map? ::position))])))

(defn- join-with-alias [{:keys [joins]} join-alias]
  (some (fn [join]
          (when (= (:alias join) join-alias)
            join))
        joins))

(defn- matching-join-source-clause [source-query clause]
  (when-let [{:keys [join-alias], :as opts} (clause-options clause)]
    (when join-alias
      (when-let [matching-join-source-query (:source-query (join-with-alias source-query join-alias))]
        (some (fn [a-clause]
                (when (= (mbql.u/update-field-options (normalize-clause clause) dissoc :join-alias)
                         (mbql.u/update-field-options (normalize-clause a-clause) dissoc :join-alias))
                  a-clause))
         (exports matching-join-source-query))))))

(defn- add-alias-info* [{:keys [source-table joins], aggregations :aggregation, :as inner-query}]
  (assert (not (:strategy inner-query)))
  (let [this-level-joins (into #{} (map :alias) joins)
        clause->position (comp (selected-clauses inner-query) normalize-clause)]
    #_(println "(u/pprint-to-str 'yellow inner-query):" (metabase.util/pprint-to-str 'yellow (dissoc inner-query :source-query :source-metadata))) ; NOCOMMIT
    (mbql.u/replace inner-query
      ;; don't rewrite anything inside any source queries or source metadata.
      (_ :guard (constantly (some (partial contains? (set &parents))
                                  [:source-query :source-metadata])))
      &match

      ;; TODO -- what about joins against native queries? I think we need to have `join-is-this-level?` logic like
      ;; below.
      [:field (field-name :guard string?) opts]
      [:field field-name (merge opts
                                {::source-table ::source
                                 ::source-alias field-name}
                                (when-let [position (clause->position &match)]
                                  {::desired-alias field-name
                                   ::position      position}))]

      [:field id-or-name opts]
      (let [field                       (when (integer? id-or-name)
                                          (qp.store/field id-or-name))
            field-name                  (or (:name field)
                                            (when (string? id-or-name)
                                              id-or-name))
            table-id                    (:table_id field)
            {:keys [join-alias]}        opts
            join-is-this-level?         (some-> join-alias this-level-joins)
            matching-join-source-clause (matching-join-source-clause inner-query &match)
            join-desired-alias          (some-> matching-join-source-clause clause-options ::desired-alias)
            table                       (cond
                                          (= table-id source-table) table-id
                                          join-is-this-level?       join-alias
                                          :else                     ::source)
            source-alias                (cond
                                          (and join-alias (not join-is-this-level?))   (format "%s__%s" join-alias field-name)
                                          (and join-is-this-level? join-desired-alias) join-desired-alias
                                          :else                                        field-name)
            desired-alias               (if join-alias
                                          (format "%s__%s" join-alias field-name)
                                          field-name)]
        (u/prog1 [:field id-or-name (merge opts
                                           {::source-table table
                                            ::source-alias source-alias}
                                           (when-let [position (clause->position &match)]
                                             {::desired-alias desired-alias
                                              ::position      position}))]
          #_(println "(pr-str <>):" (pr-str <>)) ; NOCOMMIT
          ))

      [:aggregation index & more]
      (let [position (clause->position &match)
            [opts]   more]
        (when-not position
          (throw (ex-info (tru "Aggregation does not exist at index {0}" index)
                          {:type   qp.error-type/invalid-query
                           :clause &match
                           :path   &parents
                           :query  inner-query})))
        (let [[_ ag-name _] (nth aggregations index)]
          [:aggregation index (merge opts
                                     {::desired-alias ag-name
                                      ::position      position})]))

      [:expression expression-name & more]
      (let [position (clause->position &match)
            [opts]   more]
        (assert position (format "Expression with name %s does not exist at this level" (pr-str expression-name)))
        [:expression expression-name (merge opts
                                            {::desired-alias expression-name
                                             ::position      position})]))))

;; TODO -- we should add alias info to the aggregation clauses too

(defn add-alias-info [query]
  (walk/postwalk
   (fn [form]
     (if (and (map? form)
              ((some-fn :source-query :source-table) form)
              (not (:strategy form)))
       (add-alias-info* form)
       form))
   query))

(defn uniquify-aliases
  "Make sure the `::desired-alias` of all of every selected field reference is unique."
  [inner-query]
  ;; TODO
  )

;; TODO -- raise query
