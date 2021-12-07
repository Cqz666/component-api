package com.cqz.component.common.async;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static com.cqz.component.common.async.Shop.delay;
import static java.util.stream.Collectors.toList;

public class Shop2 {
    static Random random = new Random();

    public static void main(String[] args) {
        long start = System.nanoTime();
        System.out.println(asysncFindPrices("myPhone27S"));
        long duration = (System.nanoTime() - start) / 1_000_000;
        System.out.println("Done in " + duration + " ms");
    }

    private String name;

    public Shop2(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    static List<Shop2> shops = Arrays.asList(
            new Shop2("BestPrice"),
            new Shop2("LetsSaveBig"),
            new Shop2("MyFavoriteShop"),
            new Shop2("BuyItAll")
    );

    public static List<String> findPrices(String product) {
        return shops.stream()
                .map(shop2 -> getPrice(product))
                .map(Quote::parse)
                .map(Discount::applyDiscount)
                .collect(toList());
    }

    //-------------------------------------------

    public static List<String> asysncFindPrices(String product) {
        List<CompletableFuture<String>> priceFutures =
                shops.stream()
                        .map(shop -> CompletableFuture.supplyAsync(
                                () -> getPrice(product)))
                        .map(future -> future.thenApply(Quote::parse))
                        .map(future -> future.thenCompose(quote ->
                                CompletableFuture.supplyAsync(
                                        () -> Discount.applyDiscount(quote))))
                        .collect(toList());
        return priceFutures.stream()
                .map(CompletableFuture::join)
                .collect(toList());
    }

    public static String getPrice(String product) {

        double price = calculatePrice(product);
        Discount.Code code = Discount.Code.values()[
                random.nextInt(Discount.Code.values().length)];
        return String.format("%s:%.2f:%s", product, price, code);
    }
    private static double calculatePrice(String product) {
        delay();
        return random.nextDouble() * product.charAt(0) + product.charAt(1);
    }

}
