package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class demo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> a1 = env.fromElements(
                new Person(40, "Fred", 50.5f),
                new Person(50, "Wilma", 60.5f),
                new Person(60, "Pebbles", 70.5f)
        );

        DataStream<Person> adults = a1.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return Person.age >= 18;
            }
        });
        adults.print();
        env.execute();

        //Person pp1 = (Person) Person.age;
        //System.out.println(Person.age);
    }

    public static class Person {
        static int age;
        public String name;
        float sal;

        public Person(){};
        public Person(int age,String name,float sal){
            this.age=age;
            this.name=name;
            this.sal=sal;
        };


        //Object obj=new Object();


        public String toString() {
            return "name = " + this.name.toString() + ": age = " + this.age;
        };
    }
}
