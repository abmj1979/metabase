(ns metabase.query-processor.util.nest-query-test
  (:require [clojure.test :refer :all]
            [clojure.walk :as walk]
            [metabase.driver :as driver]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor :as qp]
            [metabase.query-processor.util.add-alias-info :as add]
            [metabase.query-processor.util.nest-query :as nest-query]
            [metabase.test :as mt]))

(defn- remove-source-metadata [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :source-metadata) x)
       (dissoc x :source-metadata)
       x))
   x))

(defn- nest-expressions [query]
  (mt/with-everything-store
    (driver/with-driver :h2
      (-> query
          qp/query->preprocessed
          :query
          nest-query/nest-expressions
          remove-source-metadata))))

(deftest nest-expressions-test
  (is (query= (mt/$ids venues
                {:source-query {:source-table $$venues
                                :expressions  {:double_price [:* [:field %price {::add/source-table  $$venues
                                                                                 ::add/source-alias  "PRICE"
                                                                                 ::add/desired-alias "PRICE"
                                                                                 ::add/position      5}]
                                                              2]}
                                :fields       [[:field %id          {::add/source-table  $$venues
                                                                     ::add/source-alias  "ID"
                                                                     ::add/desired-alias "ID"
                                                                     ::add/position      0}]
                                               [:field %name        {::add/source-table  $$venues
                                                                     ::add/source-alias  "NAME"
                                                                     ::add/desired-alias "NAME"
                                                                     ::add/position      1}]
                                               [:field %category_id {::add/source-table  $$venues
                                                                     ::add/source-alias  "CATEGORY_ID"
                                                                     ::add/desired-alias "CATEGORY_ID"
                                                                     ::add/position      2}]
                                               [:field %latitude    {::add/source-table  $$venues
                                                                     ::add/source-alias  "LATITUDE"
                                                                     ::add/desired-alias "LATITUDE"
                                                                     ::add/position      3}]
                                               [:field %longitude   {::add/source-table  $$venues
                                                                     ::add/source-alias  "LONGITUDE"
                                                                     ::add/desired-alias "LONGITUDE"
                                                                     ::add/position      4}]
                                               [:field %price       {::add/source-table  $$venues
                                                                     ::add/source-alias  "PRICE"
                                                                     ::add/desired-alias "PRICE"
                                                                     ::add/position      5}]
                                               [:expression "double_price" {::add/desired-alias "double_price"
                                                                            ::add/position       6}]]}
                 :breakout     [[:field %price {::add/source-table  ::add/source
                                                ::add/source-alias  "PRICE"
                                                ::add/desired-alias "PRICE"
                                                ::add/position      0}]]
                 :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                 :fields       [[:field "double_price" {:base-type          :type/Float
                                                        ::add/source-table  ::add/source
                                                        ::add/source-alias  "double_price"
                                                        ::add/desired-alias "double_price"
                                                        ::add/position      2}]]
                 :order-by     [[:asc [:field %price {::add/source-table  ::add/source
                                                      ::add/source-alias  "PRICE"
                                                      ::add/desired-alias "PRICE"
                                                      ::add/position      0}]]]})
              (nest-expressions
               (mt/mbql-query venues
                 {:expressions {:double_price [:* $price 2]}
                  :breakout    [$price]
                  :aggregation [[:count]]
                  :fields      [[:expression "double_price"]]})))))

(deftest nest-expressions-test-2
  (is (query= (mt/$ids venues
                {:source-query {:source-table $$checkins
                                :expressions  {:double_id [:* $checkins.id 2]}
                                :fields       [$checkins.id
                                               !default.checkins.date
                                               $checkins.user_id
                                               $checkins.venue_id
                                               [:expression "double_id"]]}
                 :fields       [*double_id/Float
                                !day.checkins.date
                                !month.checkins.date]
                 :limit        1})
              (nest-expressions
               (mt/mbql-query checkins
                 {:expressions {:double_id [:* $id 2]}
                  :fields      [[:expression "double_id"]
                                !day.date
                                !month.date]
                  :limit       1})))))

(deftest nest-expressions-ignore-source-queries-test
  (testing "When 'raising' :expression clauses, only raise ones in the current level. Handle duplicate expression names correctly."
    (is (query= (mt/$ids venues
                  {:source-query
                   {:source-query
                    {:source-table $$venues
                     :expressions  {:x [:* $price 2]}
                     :fields       [$id [:expression "x"]]}
                    :expressions {:x [:* $price 4]}
                    :fields      [$id *x/Float [:expression "x"]]}
                   :fields  [$id *x/Float]
                   :limit   1})
                (nest-expressions
                 (mt/mbql-query venues
                   {:source-query {:source-table $$venues
                                   :expressions  {:x [:* $price 2]}
                                   :fields       [$id [:expression "x"]]}
                    :expressions  {:x [:* $price 4]}
                    :fields       [$id [:expression "x"]]
                    :limit        1}))))))

#_(defn- remove-qp-refs [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :qp/refs) x)
       (dissoc x :qp/refs)
       x))
   x))

(deftest nest-expressions-with-joins-test
  (testing "If there are any `:joins`, those need to be nested into the `:source-query` as well."
    (is (query= (mt/$ids venues
                  {:source-query {:source-table $$venues
                                  :joins        [{:strategy     :left-join
                                                  :condition    [:= $category_id &CategoriesStats.category_id]
                                                  :source-query {:source-table $$venues
                                                                 :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                                                [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                                [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                                 :breakout     [$category_id]
                                                                 :order-by     [[:asc $category_id]]}
                                                  :alias        "CategoriesStats"
                                                  :fields       [&CategoriesStats.category_id
                                                                 &CategoriesStats.*MaxPrice/Integer
                                                                 &CategoriesStats.*AvgPrice/Integer
                                                                 &CategoriesStats.*MinPrice/Integer]}]
                                  :expressions  {:RelativePrice [:/ $price &CategoriesStats.*AvgPrice/Integer]}
                                  :fields       [$id
                                                 $name
                                                 $category_id
                                                 $latitude
                                                 $longitude
                                                 $price
                                                 [:expression "RelativePrice"]
                                                 &CategoriesStats.category_id
                                                 &CategoriesStats.*MaxPrice/Integer
                                                 &CategoriesStats.*AvgPrice/Integer
                                                 &CategoriesStats.*MinPrice/Integer]}
                   :fields       [$id
                                  $name
                                  $category_id
                                  $latitude
                                  $longitude
                                  $price
                                  *RelativePrice/Float
                                  &CategoriesStats.category_id
                                  &CategoriesStats.*MaxPrice/Integer
                                  &CategoriesStats.*AvgPrice/Integer
                                  &CategoriesStats.*MinPrice/Integer]
                   :limit        3})
                (nest-expressions
                 (mt/mbql-query venues
                   {:fields      [$id
                                  $name
                                  $category_id
                                  $latitude
                                  $longitude
                                  $price
                                  [:expression "RelativePrice"]
                                  &CategoriesStats.category_id
                                  &CategoriesStats.*MaxPrice/Integer
                                  &CategoriesStats.*AvgPrice/Integer
                                  &CategoriesStats.*MinPrice/Integer]
                    :expressions {:RelativePrice [:/ $price &CategoriesStats.*AvgPrice/Integer]}
                    :joins       [{:strategy     :left-join
                                   :condition    [:= $category_id &CategoriesStats.category_id]
                                   :source-query {:source-table $$venues
                                                  :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                                 [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                 [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                  :breakout     [$category_id]}
                                   :alias        "CategoriesStats"
                                   :fields       :all}]
                    :limit       3}))))))

(deftest nest-expressions-eliminate-duplicate-coercion-test
  (testing "If coercion happens in the source query, don't do it a second time in the parent query (#12430)"
    (mt/with-temp-vals-in-db Field (mt/id :venues :price) {:coercion_strategy :Coercion/UNIXSeconds->DateTime
                                                           :effective_type    :type/DateTime}
      (is (query= (mt/$ids venues
                    {:source-query {:source-table $$venues
                                    :expressions  {:test [:* 1 1]}
                                    :fields       [[:field %id {::add/source-table  $$venues
                                                                ::add/source-alias  "ID"
                                                                ::add/desired-alias "ID"
                                                                ::add/position      0}]
                                                   [:field %name {::add/source-table  $$venues
                                                                  ::add/source-alias  "NAME"
                                                                  ::add/desired-alias "NAME"
                                                                  ::add/position      1}]
                                                   [:field %category_id {::add/source-table  $$venues
                                                                         ::add/source-alias  "CATEGORY_ID"
                                                                         ::add/desired-alias "CATEGORY_ID"
                                                                         ::add/position      2}]
                                                   [:field %latitude {::add/source-table  $$venues
                                                                      ::add/source-alias  "LATITUDE"
                                                                      ::add/desired-alias "LATITUDE"
                                                                      ::add/position      3}]
                                                   [:field %longitude {::add/source-table  $$venues
                                                                       ::add/source-alias  "LONGITUDE"
                                                                       ::add/desired-alias "LONGITUDE"
                                                                       ::add/position      4}]
                                                   [:field %price {:temporal-unit      :default
                                                                   ::add/source-table  $$venues
                                                                   ::add/source-alias  "PRICE"
                                                                   ::add/desired-alias "PRICE"
                                                                   ::add/position      5}]
                                                   [:expression "test" {::add/desired-alias "test"
                                                                        ::add/position     6}]]}
                     :fields       [[:field %price {:temporal-unit            :default
                                                    ::nest-query/outer-select true
                                                    ::add/source-table        ::add/source
                                                    ::add/source-alias        "PRICE"
                                                    ::add/desired-alias       "PRICE"
                                                    ::add/position            0}]
                                    [:field "test" {:base-type          :type/Float
                                                    ::add/source-table  ::add/source
                                                    ::add/source-alias  "test"
                                                    ::add/desired-alias "test"
                                                    ::add/position      1}]]
                     :limit        1})
                  (nest-expressions
                   (mt/mbql-query venues
                     {:expressions {:test ["*" 1 1]}
                      :fields      [$price
                                    [:expression "test"]]
                      :limit       1})))))))

(deftest multiple-joins-with-expressions-test
  (testing "We should be able to compile a complicated query with multiple joins and expressions correctly"
    (mt/dataset sample-dataset
      (is (query= (mt/$ids orders
                    {:source-query {:source-table $$orders
                                    :joins        [{:source-table $$products
                                                    :alias        "PRODUCTS__via__PRODUCT_ID"
                                                    :condition    [:=
                                                                   [:field %product_id {::add/source-table  $$orders
                                                                                        ::add/source-alias  "PRODUCT_ID"
                                                                                        ::add/desired-alias "PRODUCT_ID"
                                                                                        ::add/position      2}]
                                                                   [:field %products.id {:join-alias         "PRODUCTS__via__PRODUCT_ID"
                                                                                         ::add/source-table  "PRODUCTS__via__PRODUCT_ID"
                                                                                         ::add/source-alias  "ID"
                                                                                         ::add/desired-alias "PRODUCTS__via__PRODUCT_ID__ID"
                                                                                         ::add/position      10}]]
                                                    :strategy     :left-join
                                                    :fk-field-id  %product_id}]
                                    :expressions  {:pivot-grouping [:abs 0]}
                                    :fields       [[:field %id {::add/source-table  $$orders
                                                                ::add/source-alias  "ID"
                                                                ::add/desired-alias "ID"
                                                                ::add/position      0}]
                                                   [:field %user_id {::add/source-table  $$orders
                                                                     ::add/source-alias  "USER_ID"
                                                                     ::add/desired-alias "USER_ID"
                                                                     ::add/position      1}]
                                                   [:field %product_id {::add/source-table  $$orders
                                                                        ::add/source-alias  "PRODUCT_ID"
                                                                        ::add/desired-alias "PRODUCT_ID"
                                                                        ::add/position      2}]
                                                   [:field %subtotal {::add/source-table  $$orders
                                                                      ::add/source-alias  "SUBTOTAL"
                                                                      ::add/desired-alias "SUBTOTAL"
                                                                      ::add/position      3}]
                                                   [:field %tax {::add/source-table  $$orders
                                                                 ::add/source-alias  "TAX"
                                                                 ::add/desired-alias "TAX"
                                                                 ::add/position      4}]
                                                   [:field %total {::add/source-table  $$orders
                                                                   ::add/source-alias  "TOTAL"
                                                                   ::add/desired-alias "TOTAL"
                                                                   ::add/position      5}]
                                                   [:field %discount {::add/source-table  $$orders
                                                                      ::add/source-alias  "DISCOUNT"
                                                                      ::add/desired-alias "DISCOUNT"
                                                                      ::add/position      6}]
                                                   [:field %created_at {:temporal-unit      :default
                                                                        ::add/source-table  $$orders
                                                                        ::add/source-alias  "CREATED_AT"
                                                                        ::add/desired-alias "CREATED_AT"
                                                                        ::add/position      7}]
                                                   [:field %quantity {::add/source-table  $$orders
                                                                      ::add/source-alias  "QUANTITY"
                                                                      ::add/desired-alias "QUANTITY"
                                                                      ::add/position      8}]
                                                   [:expression "pivot-grouping" {::add/desired-alias "pivot-grouping"
                                                                                  ::add/position      9}]
                                                   [:field %products.id {:join-alias         "PRODUCTS__via__PRODUCT_ID"
                                                                         ::add/source-table  "PRODUCTS__via__PRODUCT_ID"
                                                                         ::add/source-alias  "ID"
                                                                         ::add/desired-alias "PRODUCTS__via__PRODUCT_ID__ID"
                                                                         ::add/position      10}]
                                                   [:field %products.category {:join-alias         "PRODUCTS__via__PRODUCT_ID"
                                                                               ::add/source-table  "PRODUCTS__via__PRODUCT_ID"
                                                                               ::add/source-alias  "CATEGORY"
                                                                               ::add/desired-alias "PRODUCTS__via__PRODUCT_ID__CATEGORY"
                                                                               ::add/position      11}]]}
                     :breakout     [[:field %products.category {:join-alias         "PRODUCTS__via__PRODUCT_ID"
                                                                ::add/source-table  ::add/source
                                                                ::add/source-alias  "PRODUCTS__via__PRODUCT_ID__CATEGORY"
                                                                ::add/desired-alias "PRODUCTS__via__PRODUCT_ID__CATEGORY"
                                                                ::add/position      0}]
                                    [:field %created_at {:temporal-unit            :year
                                                         ::nest-query/outer-select true
                                                         ::add/source-table        ::add/source
                                                         ::add/source-alias        "CREATED_AT"
                                                         ::add/desired-alias       "CREATED_AT"
                                                         ::add/position            1}]
                                    [:field "pivot-grouping" {:base-type          :type/Float
                                                              ::add/source-table  ::add/source
                                                              ::add/source-alias  "pivot-grouping"
                                                              ::add/desired-alias "pivot-grouping"
                                                              ::add/position      2}]]
                     :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                     :order-by     [[:asc [:field %products.category {:join-alias         "PRODUCTS__via__PRODUCT_ID"
                                                                      ::add/source-table  ::add/source
                                                                      ::add/source-alias  "PRODUCTS__via__PRODUCT_ID__CATEGORY"
                                                                      ::add/desired-alias "PRODUCTS__via__PRODUCT_ID__CATEGORY"
                                                                      ::add/position      0}]]
                                    [:asc [:field %created_at {:temporal-unit            :year
                                                               ::nest-query/outer-select true
                                                               ::add/source-table        ::add/source
                                                               ::add/source-alias        "CREATED_AT"
                                                               ::add/desired-alias       "CREATED_AT"
                                                               ::add/position            1}]]
                                    [:asc [:field "pivot-grouping" {:base-type          :type/Float
                                                                    ::add/source-table  ::add/source
                                                                    ::add/source-alias  "pivot-grouping"
                                                                    ::add/desired-alias "pivot-grouping"
                                                                    ::add/position      2}]]]})
                  (nest-expressions
                   (mt/mbql-query orders
                     {:aggregation [[:aggregation-options [:count] {:name "count"}]]
                      :breakout    [&PRODUCTS__via__PRODUCT_ID.products.category
                                    !year.created_at
                                    [:expression "pivot-grouping"]]
                      :expressions {:pivot-grouping [:abs 0]}
                      :order-by    [[:asc &PRODUCTS__via__PRODUCT_ID.products.category]
                                    [:asc !year.created_at]
                                    [:asc [:expression "pivot-grouping"]]]
                      :joins       [{:source-table $$products
                                     :strategy     :left-join
                                     :alias        "PRODUCTS__via__PRODUCT_ID"
                                     :fk-field-id  %product_id
                                     :condition    [:= $product_id &PRODUCTS__via__PRODUCT_ID.products.id]}]})))))))
