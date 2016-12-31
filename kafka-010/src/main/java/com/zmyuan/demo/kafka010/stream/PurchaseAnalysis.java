package com.zmyuan.demo.kafka010.stream;

import com.zmyuan.demo.kafka010.stream.model.Item;
import com.zmyuan.demo.kafka010.stream.model.Order;
import com.zmyuan.demo.kafka010.stream.model.User;
import com.zmyuan.demo.kafka010.stream.serdes.SerdesFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

/**
 * Created by zdb on 2016/12/31.
 */
public class PurchaseAnalysis {

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka0:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper0:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
        KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
        KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");
//		itemTable.toStream().foreach((String itemName, Item item) -> System.out.printf("Item info %s-%s-%s-%s\n", item.getItemName(), item.getAddress(), item.getType(), item.getPrice()));
        KTable<String, Long> kTable = orderStream
                .leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
                // 过滤掉年龄不在 18-35之间的用户
                .filter((String userName, OrderUser orderUser) ->
                        orderUser.userAddress != null && orderUser.age >= 18 && orderUser.age <= 35)
                .map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
                .through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> {
//                    System.out.println(String.format("分区数======================================%d", numPartitions));
                    return (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions;
                }, "orderuser-repartition-by-item")
                .leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
//				.filter((String item, OrderUserItem orderUserItem) -> StringUtils.compare(orderUserItem.userAddress, orderUserItem.itemAddress) == 0)
//				.foreach((String itemName, OrderUserItem orderUserItem) -> System.out.printf("%s-%s-%s-%s\n", itemName, orderUserItem.itemAddress, orderUserItem.userName, orderUserItem.userAddress))
                // 将品类, 商品名，价格 作为key
                .map((String item, OrderUserItem orderUserItem) -> KeyValue.<String, Long>pair(String.format("%s,%s,%s", orderUserItem.getItemType(), orderUserItem.getItemName(), orderUserItem.getItemPrice()),
                        // 订单数，商品数，总价
                        Long.getLong(orderUserItem.getQuantity() + "")))
                .groupByKey(Serdes.String(), Serdes.Long())
                // window 聚合
//                .aggregate(() -> 0L, (aggKey, value, aggregate) -> aggregate + 1L,
//                        TimeWindows.of(1000 * 60 * 60).advanceBy(5000),
//                        Serdes.Long(),
//                        "Counts").toStream().map(KeyValue.pair())

                .reduce((Long v1, Long v2) -> {
                    return v1 + v2;
                }, "item-amount-state-store");

        KTable<Windowed<String>, ArrayList> kTable2 = kTable.toStream().map((String key, Long val) -> {
            String[] keys = key.split(",");
            return KeyValue.pair(keys[0], key + "," + val);
        }).groupByKey()
                // 数组中保存10个最大的10个值
                .aggregate(() -> new ArrayList(), (aggKey, value, aggregate) -> {
                    if(aggregate.size() < 10) {
                        aggregate.add(value);
                    } else {
                        // 移除最小的一个，添加新值
                        Collections.sort(aggregate, new Comparator() {
                            @Override
                            public int compare(Object o1, Object o2) {
                                // 比较销量
                                Long quantity1 = getQuantity((String)o1);
                                Long quantity2 = getQuantity((String)o2);

                                return Integer.parseInt((quantity1 - quantity2) + "");
                            }
                        });
                        aggregate.remove(aggregate.get(9));
                        aggregate.add(value);
                    }
                    return aggregate;
                },TimeWindows.of(1000 * 60 * 60).advanceBy(5000),
                        SerdesFactory.serdFrom(ArrayList.class),
                        "item_counts");
        kTable2.foreach((Windowed<String> window, ArrayList top10) -> {
            System.out.println(String.format("key=%s, value=%s, start=%d, end=%d\n",window.key(), printArrayList(top10), window.window().start(), window.window().end()));
        });
        kTable2.toStream().map((Windowed<String> window, ArrayList top10) -> {
                    return new KeyValue<String, String>(window.key(),
                            String.format("key=%s, value=%s, start=%d, end=%d\n",window.key(), printArrayList(top10), window.window().start(), window.window().end()));
                }).to("type_top10");

        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    public static Long getQuantity(String val) {
        return Long.parseLong(val.substring(val.lastIndexOf(",") + 1));
    }

    public static String printArrayList(ArrayList list) {
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        boolean isFirst = true;
        for(Object obj : list) {
            if(isFirst) {
                sb.append(obj.toString());
                isFirst = false;
            } else {
                sb.append(obj.toString());
                sb.append("\n");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static class OrderUser {
        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String userAddress;
        private String gender;
        private int age;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public long getTransactionDate() {
            return transactionDate;
        }

        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getUserAddress() {
            return userAddress;
        }

        public void setUserAddress(String userAddress) {
            this.userAddress = userAddress;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public static OrderUser fromOrder(Order order) {
            OrderUser orderUser = new OrderUser();
            if(order == null) {
                return orderUser;
            }
            orderUser.userName = order.getUserName();
            orderUser.itemName = order.getItemName();
            orderUser.transactionDate = order.getTransactionDate();
            orderUser.quantity = order.getQuantity();
            return orderUser;
        }

        public static OrderUser fromOrderUser(Order order, User user) {
            OrderUser orderUser = fromOrder(order);
            if(user == null) {
                return orderUser;
            }
            orderUser.gender = user.getGender();
            orderUser.age = user.getAge();
            orderUser.userAddress = user.getAddress();
            return orderUser;
        }
    }

    public static class OrderUserItem {
        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String userAddress;
        private String gender;
        private int age;
        private String itemAddress;
        private String itemType;
        private double itemPrice;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public long getTransactionDate() {
            return transactionDate;
        }

        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getUserAddress() {
            return userAddress;
        }

        public void setUserAddress(String userAddress) {
            this.userAddress = userAddress;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getItemAddress() {
            return itemAddress;
        }

        public void setItemAddress(String itemAddress) {
            this.itemAddress = itemAddress;
        }

        public String getItemType() {
            return itemType;
        }

        public void setItemType(String itemType) {
            this.itemType = itemType;
        }

        public double getItemPrice() {
            return itemPrice;
        }

        public void setItemPrice(double itemPrice) {
            this.itemPrice = itemPrice;
        }

        public static OrderUserItem fromOrderUser(OrderUser orderUser) {
            OrderUserItem orderUserItem = new OrderUserItem();
            if(orderUser == null) {
                return orderUserItem;
            }
            orderUserItem.userName = orderUser.userName;
            orderUserItem.itemName = orderUser.itemName;
            orderUserItem.transactionDate = orderUser.transactionDate;
            orderUserItem.quantity = orderUser.quantity;
            orderUserItem.userAddress = orderUser.userAddress;
            orderUserItem.gender = orderUser.gender;
            orderUserItem.age = orderUser.age;
            return orderUserItem;
        }

        public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
            OrderUserItem orderUserItem = fromOrderUser(orderUser);
            if(item == null) {
                return orderUserItem;
            }
            orderUserItem.itemAddress = item.getAddress();
            orderUserItem.itemType = item.getType();
            orderUserItem.itemPrice = item.getPrice();
            return orderUserItem;
        }
    }
}
