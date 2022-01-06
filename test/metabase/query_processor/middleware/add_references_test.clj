(ns metabase.query-processor.middleware.add-references-test
  (:require [clojure.test :refer :all]
            [metabase.query-processor :as qp]
            [metabase.query-processor.middleware.add-references :as add-references]
            [metabase.test :as mt]
            [metabase.driver :as driver]
            [metabase.models.database :refer [Database]]
            [toucan.db :as db]
            [dev.debug-qp :as debug-qp]
            [medley.core :as m]))

(deftest selected-references-aggregations-test
  (testing "Make sure references to this-level aggregations work correctly"
    (is (= {[:aggregation 0]           {:position 0, :alias "COUNT"}
            [:aggregation 1]           {:position 1, :alias "sum_x"}
            [:field "y" :type/Integer] {:position 2}}
           (#'add-references/selected-references
            {:aggregation [[:aggregation-options [:count] {:name "COUNT"}]
                           [:aggregation-options [:sum [:field "x" :type/Integer]] {:name "sum_x"}]]
             :fields      [[:field "y" :type/Integer]]}))))
  (testing "Make sure all aliases get uniquified"
    (is (= {:type  :query
            :query {:source-query {:native "SELECT * FROM WHATEVER"}
                    :aggregation  [[:aggregation-options [:count] {:name "COUNT"}]
                                   [:aggregation-options [:sum [:field "x" :type/Integer]] {:name "sum_x"}]]
                    :fields       [[:field "COUNT" :type/Integer]]
                    :qp/refs      {[:aggregation 0]               {:position 0, :alias "COUNT"}
                                   [:aggregation 1]               {:position 1, :alias "sum_x"}
                                   [:field "COUNT" :type/Integer] {:position 2, :alias "COUNT_2"}}}}
           (#'add-references/add-references
            {:type  :query
             :query {:source-query {:native "SELECT * FROM WHATEVER"}
                     :aggregation  [[:aggregation-options [:count] {:name "COUNT"}]
                                    [:aggregation-options [:sum [:field "x" :type/Integer]] {:name "sum_x"}]]
                     :fields       [[:field "COUNT" :type/Integer]]}})))))

(deftest field-name-test
  (is (= {:type  :query
          :query {:source-query {}
                  :fields       [[:field "count" {:base-type :type/BigInteger}]]
                  :qp/refs      {[:field "count" {:base-type :type/BigInteger}]
                                 {:position 0, :alias "count"}}}}
         (add-references/add-references
          {:type  :query
           :query {:source-query {}
                   :fields       [[:field "count" {:base-type :type/BigInteger}]]}}))))

(deftest join-in-join-test
  (mt/dataset sample-dataset
    (mt/with-everything-store
      (mt/test-driver :h2
        (is (query= (mt/$ids orders
                      {$discount                  {:alias "DISCOUNT"}
                       $subtotal                  {:alias "SUBTOTAL"}
                       &Q2.products.category      {:position 0, :alias "Q2__P2__CATEGORY", :source {:table "Q2", :alias "P2__CATEGORY"}}
                       $quantity                  {:alias "QUANTITY"}
                       $user_id                   {:alias "USER_ID"}
                       $total                     {:alias "TOTAL"}
                       $tax                       {:alias "TAX"}
                       $id                        {:alias "ID"}
                       !default.orders.created_at {:alias "CREATED_AT"}
                       $product_id                {:alias "PRODUCT_ID"}})
                    (get-in (add-references/add-references
                             (mt/mbql-query orders
                               {:fields [&Q2.products.category]
                                :joins  [{:strategy     :left-join
                                          :condition    [:= $products.category &Q2.products.category]
                                          :alias        "Q2"
                                          :source-query {:source-table $$reviews
                                                         :aggregation  [[:aggregation-options
                                                                         [:avg $reviews.rating]
                                                                         {:name "avg"}]]
                                                         :breakout     [&P2.products.category]
                                                         :joins        [{:strategy     :left-join
                                                                         :source-table $$products
                                                                         :condition    [:=
                                                                                        $reviews.product_id
                                                                                        &P2.products.id]
                                                                         :alias        "P2"}]}}]}))
                            [:query :qp/refs])))))))

(defn- add-references [{database-id :database, :as query}]
  (mt/with-everything-store
    (driver/with-driver (db/select-one-field :engine Database :id database-id)
      (add-references/add-references query))))

(deftest expressions-test
  (is (= (mt/mbql-query venues
           {:expressions {"double_id" [:* $id 2]}
            :qp/refs     {$category_id {:alias "CATEGORY_ID"}
                          $id          {:alias "ID"}
                          $latitude    {:alias "LATITUDE"}
                          $longitude   {:alias "LONGITUDE"}
                          $name        {:alias "NAME"}
                          $price       {:alias "PRICE"}}})
         (add-references
          (mt/mbql-query venues
            {:expressions {"double_id" [:* $id 2]}}))))

  (testing ":expression in :fields"
    (is (= (mt/mbql-query venues
             {:expressions {"double_id" [:* $id 2]}
              :fields      [[:expression "double_id"]]
              :qp/refs     {$category_id              {:alias "CATEGORY_ID"}
                            $id                       {:alias "ID"}
                            $latitude                 {:alias "LATITUDE"}
                            $longitude                {:alias "LONGITUDE"}
                            $name                     {:alias "NAME"}
                            $price                    {:alias "PRICE"}
                            [:expression "double_id"] {:position 0, :alias "double_id"}}})
           (add-references
            (mt/mbql-query venues
              {:fields      [[:expression "double_id"]]
               :expressions {"double_id" [:* $id 2]}})))))

  (testing "Inside a nested query"
    (is (= (mt/mbql-query venues
             {:source-query {:source-table $$venues
                             :expressions  {:double_id [:* $id 2]}
                             :fields       [$id $name $category_id $latitude $longitude $price [:expression "double_id"]]
                             :qp/refs      {$category_id              {:position 2, :alias "CATEGORY_ID"}
                                            $id                       {:position 0, :alias "ID"}
                                            $latitude                 {:position 3, :alias "LATITUDE"}
                                            $longitude                {:position 4, :alias "LONGITUDE"}
                                            $name                     {:position 1, :alias "NAME"}
                                            $price                    {:position 5, :alias "PRICE"}
                                            [:expression "double_id"] {:position 6, :alias "double_id"}}}
              :fields       [$id $name $category_id $latitude $longitude $price *double_id/Float]
              :qp/refs      {$id                                           {:position 0, :alias "ID", :source {:table "source", :alias "ID"}},
                             $name                                         {:position 1, :alias "NAME", :source {:table "source", :alias "NAME"}},
                             $category_id                                  {:position 2, :alias "CATEGORY_ID", :source {:table "source", :alias "CATEGORY_ID"}},
                             $latitude                                     {:position 3, :alias "LATITUDE", :source {:table "source", :alias "LATITUDE"}},
                             $longitude                                    {:position 4, :alias "LONGITUDE", :source {:table "source", :alias "LONGITUDE"}},
                             $price                                        {:position 5, :alias "PRICE", :source {:table "source", :alias "PRICE"}},
                             [:field "double_id" {:base-type :type/Float}] {:position 6, :alias "double_id", :source {:table "source", :alias "double_id"}}}})
           (add-references
            (mt/mbql-query venues
              {:source-query {:source-table $$venues
                              :expressions  {:double_id [:* $id 2]}
                              :fields       [$id $name $category_id $latitude $longitude $price [:expression "double_id"]]}
               :fields       [$id $name $category_id $latitude $longitude $price *double_id/Float]}))))))

;; TODO -- native query with source metadata test.

(deftest mega-query-refs-test
  (testing "Should generate correct SQL for joins against source queries that contain joins (#12928)"
    (mt/dataset sample-dataset
      (is (= '{:fields
               [&P1.products.category
                &People.people.source
                *count/BigInteger
                &Q2.products.category
                &Q2.*avg/Integer]

               :source-query
               {:source-table $$orders
                :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                :breakout     [&P1.products.category &People.people.source]
                :order-by     [[:asc &P1.products.category] [:asc &People.people.source]]
                :joins        [{:strategy     :left-join
                                :source-table $$products
                                :condition    [:= $product_id &P1.products.id]
                                :alias        "P1"}
                               {:strategy     :left-join
                                :source-table $$people
                                :condition    [:= $user_id &People.people.id]
                                :alias        "People"}]
                :qp/refs      {!default.&P1.products.created_at   {:source {:table "P1", :alias "CREATED_AT"}, :alias "P1__CREATED_AT"}
                               !default.&People.people.birth_date {:source {:table "People", :alias "BIRTH_DATE"}, :alias "People__BIRTH_DATE"}
                               !default.&People.people.created_at {:source {:table "People", :alias "CREATED_AT"}, :alias "People__CREATED_AT"}
                               !default.created_at                {:alias "CREATED_AT"}
                               $discount                          {:alias "DISCOUNT"}
                               $id                                {:alias "ID"}
                               $product_id                        {:alias "PRODUCT_ID"}
                               $quantity                          {:alias "QUANTITY"}
                               $subtotal                          {:alias "SUBTOTAL"}
                               $tax                               {:alias "TAX"}
                               $total                             {:alias "TOTAL"}
                               $user_id                           {:alias "USER_ID"}
                               &P1.products.category              {:position 0, :alias "P1__CATEGORY", :source {:table "P1", :alias "CATEGORY"}}
                               &P1.products.ean                   {:source {:table "P1", :alias "EAN"}, :alias "P1__EAN"}
                               &P1.products.id                    {:source {:table "P1", :alias "ID"}, :alias "P1__ID"}
                               &P1.products.price                 {:source {:table "P1", :alias "PRICE"}, :alias "P1__PRICE"}
                               &P1.products.rating                {:source {:table "P1", :alias "RATING"}, :alias "P1__RATING"}
                               &P1.products.title                 {:source {:table "P1", :alias "TITLE"}, :alias "P1__TITLE"}
                               &P1.products.vendor                {:source {:table "P1", :alias "VENDOR"}, :alias "P1__VENDOR"}
                               &People.people.address             {:source {:table "People", :alias "ADDRESS"}, :alias "People__ADDRESS"}
                               &People.people.city                {:source {:table "People", :alias "CITY"}, :alias "People__CITY"}
                               &People.people.email               {:source {:table "People", :alias "EMAIL"}, :alias "People__EMAIL"}
                               &People.people.id                  {:source {:table "People", :alias "ID"}, :alias "People__ID"}
                               &People.people.latitude            {:source {:table "People", :alias "LATITUDE"}, :alias "People__LATITUDE"}
                               &People.people.longitude           {:source {:table "People", :alias "LONGITUDE"}, :alias "People__LONGITUDE"}
                               &People.people.name                {:source {:table "People", :alias "NAME"}, :alias "People__NAME"}
                               &People.people.password            {:source {:table "People", :alias "PASSWORD"}, :alias "People__PASSWORD"}
                               &People.people.source              {:position 1, :alias "People__SOURCE", :source {:table "People", :alias "SOURCE"}}
                               &People.people.state               {:source {:table "People", :alias "STATE"}, :alias "People__STATE"}
                               &People.people.zip                 {:source {:table "People", :alias "ZIP"}, :alias "People__ZIP"}
                               [:aggregation 0]                   {:position 2, :alias "count"}}}

               :joins
               [{:strategy     :left-join
                 :condition    [:= &P1.products.category &Q2.products.category]
                 :alias        "Q2"
                 :source-query {:source-table $$reviews
                                :aggregation  [[:aggregation-options [:avg $reviews.rating] {:name "avg"}]]
                                :breakout     [&P2.products.category]
                                :joins        [{:strategy     :left-join
                                                :source-table $$products
                                                :condition    [:= $reviews.product_id &P2.products.id]
                                                :alias        "P2"}]
                                :qp/refs      {&P2.products.vendor              {:source {:table "P2", :alias "VENDOR"}, :alias "P2__VENDOR"}
                                               &P2.products.id                  {:source {:table "P2", :alias "ID"}, :alias "P2__ID"}
                                               &P2.products.ean                 {:source {:table "P2", :alias "EAN"}, :alias "P2__EAN"}
                                               &P2.products.category            {:position 0, :alias "P2__CATEGORY", :source {:table "P2", :alias "CATEGORY"}}
                                               $reviews.rating                  {:alias "RATING"}
                                               $reviews.body                    {:alias "BODY"}
                                               $reviews.product_id              {:alias "PRODUCT_ID"}
                                               $reviews.id                      {:alias "ID"}
                                               !default.reviews.created_at      {:alias "CREATED_AT"}
                                               &P2.products.price               {:source {:table "P2", :alias "PRICE"}, :alias "P2__PRICE"}
                                               $reviews.reviewer                {:alias "REVIEWER"}
                                               !default.&P2.products.created_at {:source {:table "P2", :alias "CREATED_AT"}, :alias "P2__CREATED_AT"}
                                               &P2.products.title               {:source {:table "P2", :alias "TITLE"}, :alias "P2__TITLE"}
                                               &P2.products.rating              {:source {:table "P2", :alias "RATING"}, :alias "P2__RATING"}
                                               [:aggregation 0]                 {:alias "avg", :position 1}}}}]
               :limit   2
               :qp/refs {&P1.products.category {:position 0, :alias "P1__CATEGORY", :source {:table "source", :alias "P1__CATEGORY"}}
                         &People.people.source {:position 1, :alias "People__SOURCE", :source {:table "source", :alias "People__SOURCE"}}
                         *count/BigInteger     {:position 2, :alias "count", :source {:alias "count", :table "source"}}
                         &Q2.products.category {:position 3, :alias "Q2__P2__CATEGORY", :source {:table "Q2", :alias "P2__CATEGORY"}}
                         &Q2.*avg/Integer      {:position 4, :alias "Q2__avg", :source {:table "Q2", :alias "avg"}}}}
             (-> (mt/mbql-query nil
                   {:fields       [&P1.products.category
                                   &People.people.source
                                   [:field "count" {:base-type :type/BigInteger}]
                                   &Q2.products.category
                                   [:field "avg" {:base-type :type/Integer, :join-alias "Q2"}]]
                    :source-query {:source-table $$orders
                                   :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                                   :breakout     [&P1.products.category
                                                  &People.people.source]
                                   :order-by     [[:asc &P1.products.category]
                                                  [:asc &People.people.source]]
                                   :joins        [{:strategy     :left-join
                                                   :source-table $$products
                                                   :condition    [:= $orders.product_id &P1.products.id]
                                                   :alias        "P1"}
                                                  {:strategy     :left-join
                                                   :source-table $$people
                                                   :condition    [:= $orders.user_id &People.people.id]
                                                   :alias        "People"}]}
                    :joins        [{:strategy     :left-join
                                    :condition    [:= &P1.products.category &Q2.products.category]
                                    :alias        "Q2"
                                    :source-query {:source-table $$reviews
                                                   :aggregation  [[:aggregation-options [:avg $reviews.rating] {:name "avg"}]]
                                                   :breakout     [&P2.products.category]
                                                   :joins        [{:strategy     :left-join
                                                                   :source-table $$products
                                                                   :condition    [:= $reviews.product_id &P2.products.id]
                                                                   :alias        "P2"}]}}]
                    :limit        2})
                 qp/query->preprocessed
                 add-references
                 (m/dissoc-in [:query :source-metadata])
                 (m/dissoc-in [:query :joins 0 :source-metadata])
                 debug-qp/to-mbql-shorthand
                 last))))))
