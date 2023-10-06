package com.gary.flinkexamples;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataSetFlinkTest {
    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /**
     * WordCount test
     * @throws Exception
     */
    @Test
    void givenDataSetWhenExecuteWordCountThenRequrnWordCount() throws Exception{
        //Sample data set
        List<String> lines = Arrays.asList("This is a first sentence", "this is a second sentence with a one word");

        //Run the word count
        DataSet<Tuple2<String, Integer>> result = WordCount.startWordCount(env, lines);

        //Result
        List<Tuple2<String, Integer>> collect = result.collect();
        assertThat(collect).containsExactlyInAnyOrder(
                new Tuple2<>("a", 3),
                new Tuple2<>("sentence", 2),
                new Tuple2<>("word", 1),
                new Tuple2<>("is", 2),
                new Tuple2<>("this", 2),
                new Tuple2<>("second", 1),
                new Tuple2<>("first", 1),
                new Tuple2<>("with", 1),
                new Tuple2<>("one", 1)
        );
    }

    /**
     * Filter and Reduce test
     * @throws Exception
     */
    @Test
    void givenDataSetWhenUseFilterReduceThenSumAmountsThatAreOnlyAboveThreshold() throws Exception{
        int threshold = 30;
        DataSet<Integer> amounts = env.fromElements(1, 29, 40, 13, 50, 24, 30, 31);

        List<Integer> totalAmount = amounts.filter(f -> f > threshold).reduce((a1, a2) -> a1 + a2).collect();

        assertThat(totalAmount.get(0)).isEqualTo(121);
    }

    /**
     * Object Map test
     *
     * Warning: chill-java api required for objects mapping in array list serializer
     * @throws Exception
     */
    @Test
    void givenDataSetWithComplexObjectsWhenMapTGetOneFieldThenShouldReturnListHaveProperElements() throws Exception {
        DataSet<Person> personDataSet = env.fromCollection(Arrays.asList(
                new Person(23, "Tom"),
                new Person(24, "Teagan"),
                new Person(27, "Shaheed"),
                new Person(47, "Gary")
        ));

        List<Integer> ages = personDataSet.map(p -> p.getAge()).collect();

        assertThat(ages).hasSize(4);
        assertThat(ages).contains(23,24,27,47);
    }

    /**
     * Sort test
     */
    @Test
    void givenDataSetWhenSortItByOneFieldThenShouldReturnSortedDataSet() throws Exception {
        Tuple2<Integer, String> person1 = new Tuple2<>(4, "Tom");
        Tuple2<Integer, String> person2 = new Tuple2<>(5, "Scott");
        Tuple2<Integer, String> person3 = new Tuple2<>(200, "Michael");
        Tuple2<Integer, String> person4 = new Tuple2<>(1, "Jack");
        DataSet<Tuple2<Integer, String>> transactions = env.fromElements(person4, person2, person3, person1);

        List<Tuple2<Integer, String>> sorted = transactions.sortPartition(new IdKeySelectorTransaction().toString(), Order.ASCENDING).collect();

        assertThat(sorted).containsExactly(person4, person1, person2, person3);
    }

    /**
     * Join test
     * @throws Exception
     */
    @Test
    void givenTwoDataSetsWhenJoinUsingIdThenProduceJoinedData() throws Exception {
        Tuple3<Integer, String, String> address = new Tuple3<>(1, "5th Avenue", "London");
        DataSet<Tuple3<Integer, String, String>> addresses = env.fromElements(address);

        Tuple2<Integer, String> firstTransaction = new Tuple2<>(1, "Transaction_1");
        DataSet<Tuple2<Integer, String>> transactions = env.fromElements(firstTransaction, new Tuple2<>(12, "Transaction_2"));

        List<Tuple2<Tuple2<Integer, String>, Tuple3<Integer, String, String>>> joined =
                transactions.join(addresses).where(String.valueOf(new IdKeySelectorTransaction())).equalTo(new IdKeySelectorAddress()).collect();

        assertThat(joined).hasSize(1);
        assertThat(joined).contains(new Tuple2<>(firstTransaction, address));
    }
}