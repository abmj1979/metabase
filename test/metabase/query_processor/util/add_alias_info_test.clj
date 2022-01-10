(ns metabase.query-processor.util.add-alias-info-test
  (:require [metabase.query-processor.util.add-alias-info :as add]
            [clojure.test :refer :all]
            [metabase.query-processor :as qp]
            [metabase.test :as mt]
            [clojure.walk :as walk]))

(defn- remove-source-metadata [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :source-metadata) x)
       (dissoc x :source-metadata)
       x))
   x))

(defn- add-alias-info [query]
  (mt/with-everything-store
    (-> query qp/query->preprocessed add/add-alias-info remove-source-metadata (dissoc :middleware))))

(deftest join-in-source-query-test
  (is (query= (mt/mbql-query venues
                {:source-query {:source-table $$venues
                                :joins        [{:strategy     :left-join
                                                :source-table $$categories
                                                :alias        "Cat"
                                                :condition    [:=
                                                               [:field %category_id {::add/source-table  $$venues
                                                                                     ::add/source-alias  "CATEGORY_ID"}]
                                                               [:field %categories.id {:join-alias         "Cat"
                                                                                       ::add/source-table  "Cat"
                                                                                       ::add/source-alias  "ID"}]]}]
                                :fields       [[:field %id {::add/source-table  $$venues
                                                            ::add/source-alias  "ID"
                                                            ::add/desired-alias "ID"
                                                            ::add/position      0}]
                                               [:field %categories.name {:join-alias         "Cat"
                                                                         ::add/source-table  "Cat"
                                                                         ::add/source-alias  "NAME"
                                                                         ::add/desired-alias "Cat__NAME"
                                                                         ::add/position      1}]]}
                 :breakout     [[:field %categories.name {:join-alias         "Cat"
                                                          ::add/source-table  ::add/source
                                                          ::add/source-alias  "Cat__NAME"
                                                          ::add/desired-alias "Cat__NAME"
                                                          ::add/position      0}]]
                 :order-by     [[:asc [:field %categories.name {:join-alias         "Cat"
                                                                ::add/source-table  ::add/source
                                                                ::add/source-alias  "Cat__NAME"
                                                                ::add/desired-alias "Cat__NAME"
                                                                ::add/position      0}]]]
                 :limit        1})
              (add-alias-info
               (mt/mbql-query venues
                 {:source-query {:source-table $$venues
                                 :joins        [{:strategy     :left-join
                                                 :source-table $$categories
                                                 :alias        "Cat"
                                                 :condition    [:= $category_id &Cat.categories.id]}]
                                 :fields       [$id
                                                &Cat.categories.name]}
                  :breakout     [&Cat.categories.name]
                  :limit        1})))))

(deftest multiple-joins-test
  (mt/dataset sample-dataset
    (is (query= (mt/mbql-query orders
                  {:source-query {:source-table $$orders
                                  :joins        [{:source-table $$products
                                                  :alias        "P1"
                                                  :condition    [:=
                                                                 [:field %product_id {::add/source-alias "PRODUCT_ID"
                                                                                      ::add/source-table $$orders}]
                                                                 [:field %products.id {:join-alias        "P1"
                                                                                       ::add/source-alias "ID"
                                                                                       ::add/source-table "P1"}]]
                                                  :strategy     :left-join}]
                                  :fields       [[:field %products.category {:join-alias         "P1"
                                                                             ::add/desired-alias "P1__CATEGORY"
                                                                             ::add/position      0
                                                                             ::add/source-alias  "CATEGORY"
                                                                             ::add/source-table  "P1"}]]}
                   :joins        [{:source-query {:source-table $$reviews
                                                  :joins        [{:source-table $$products
                                                                  :alias        "P2"
                                                                  :condition    [:=
                                                                                 [:field
                                                                                  %reviews.product_id
                                                                                  {::add/source-alias "PRODUCT_ID"
                                                                                   ::add/source-table $$reviews}]
                                                                                 [:field
                                                                                  %products.id
                                                                                  {:join-alias        "P2"
                                                                                   ::add/source-alias "ID"
                                                                                   ::add/source-table "P2"}]]
                                                                  :strategy     :left-join}]
                                                  :fields       [[:field
                                                                  %products.category
                                                                  {:join-alias         "P2"
                                                                   ::add/desired-alias "P2__CATEGORY"
                                                                   ::add/position      0
                                                                   ::add/source-alias  "CATEGORY"
                                                                   ::add/source-table  "P2"}]]}
                                   :alias        "Q2"
                                   :condition    [:=
                                                  [:field %products.category {:join-alias         "P1"
                                                                              ::add/desired-alias "P1__CATEGORY"
                                                                              ::add/position      0
                                                                              ::add/source-alias  "P1__CATEGORY"
                                                                              ::add/source-table  ::add/source}]
                                                  [:field %products.category {:join-alias        "Q2"
                                                                              ::add/source-alias "P2__CATEGORY"
                                                                              ::add/source-table "Q2"}]]
                                   :strategy     :left-join}]
                   :fields       [[:field %products.category {:join-alias         "P1"
                                                              ::add/desired-alias "P1__CATEGORY"
                                                              ::add/position      0
                                                              ::add/source-alias  "P1__CATEGORY"
                                                              ::add/source-table  ::add/source}]]
                   :limit        1})
                (add-alias-info
                 (mt/mbql-query orders
                   {:fields       [&P1.products.category]
                    :source-query {:source-table $$orders
                                   :fields       [&P1.products.category]
                                   :joins        [{:strategy     :left-join
                                                   :source-table $$products
                                                   :condition    [:= $product_id &P1.products.id]
                                                   :alias        "P1"}]}
                    :joins        [{:strategy     :left-join
                                    :condition    [:= &P1.products.category &Q2.products.category]
                                    :alias        "Q2"
                                    :source-query {:source-table $$reviews
                                                   :fields       [&P2.products.category]
                                                   :joins        [{:strategy     :left-join
                                                                   :source-table $$products
                                                                   :condition    [:= $reviews.product_id &P2.products.id]
                                                                   :alias        "P2"}]}}]
                    :limit        1}))))))
