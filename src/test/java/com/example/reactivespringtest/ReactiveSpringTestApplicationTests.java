package com.example.reactivespringtest;

import com.example.reactivespringtest.model.Player;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@SpringBootTest
class ReactiveSpringTestApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void createFlux_just() {
        Flux<String> fruitFlux = Flux
                .just("Apple", "Orange", "Grape", "Banana", "Strawberry");

        StepVerifier.create(fruitFlux)
                .expectNext("Apple")
                .expectNext("Orange")
                .expectNext("Grape")
                .expectNext("Banana")
                .expectNext("Strawberry")
                .verifyComplete();
    }

    @Test
    public void createFlux_fromArray() {
        Flux<String> fruitFlux = Flux.fromArray(new String[]{
                "Apple", "Orange", "Grape", "Banana", "Strawberry"});

        StepVerifier.create(fruitFlux)
                .expectNext("Apple")
                .expectNext("Orange")
                .expectNext("Grape")
                .expectNext("Banana")
                .expectNext("Strawberry")
                .verifyComplete();
    }

    @Test
    public void createFlux_fromIterable() {
        Flux<String> fruitFlux = Flux.fromIterable(List.of(
                "Apple", "Orange", "Grape", "Banana", "Strawberry"));

        StepVerifier.create(fruitFlux)
                .expectNext("Apple")
                .expectNext("Orange")
                .expectNext("Grape")
                .expectNext("Banana")
                .expectNext("Strawberry")
                .verifyComplete();
    }

    @Test
    public void createFlux_fromStream() {
        Flux<String> fruitFlux = Flux.fromStream(Stream.of(
                "Apple", "Orange", "Grape", "Banana", "Strawberry"));

        StepVerifier.create(fruitFlux)
                .expectNext("Apple")
                .expectNext("Orange")
                .expectNext("Grape")
                .expectNext("Banana")
                .expectNext("Strawberry")
                .verifyComplete();
    }

    @Test
    public void createFlux_range() {
        Flux<Integer> intervalFlux = Flux.range(1, 5);

        StepVerifier.create(intervalFlux)
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .expectNext(4)
                .expectNext(5)
                .verifyComplete();
    }

    @Test
    public void createFlux_interval() {
        Flux<Long> intervalFlux = Flux.interval(Duration.ofSeconds(1)).take(5);

        StepVerifier.create(intervalFlux)
                .expectNext(0L)
                .expectNext(1L)
                .expectNext(2L)
                .expectNext(3L)
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    public void mergeFluxes() {
        Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa")
                .delayElements(Duration.ofMillis(500));
        Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apple")
                .delaySubscription(Duration.ofMillis(250))
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergedFlux = characterFlux.mergeWith(foodFlux);

        StepVerifier.create(mergedFlux)
                .expectNext("Garfield", "Lasagna")
                .expectNext("Kojak", "Lollipops")
                .expectNext("Barbossa", "Apple")
                .verifyComplete();
    }

    @Test
    public void zipFluxes() {
        Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa");
        Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apple");

        Flux<Tuple2<String, String>> zippedFlux = Flux.zip(characterFlux, foodFlux);

        StepVerifier.create(zippedFlux)
                .expectNextMatches(t -> t.getT1().equals("Garfield")
                        && t.getT2().equals("Lasagna"))
                .expectNextMatches(t -> t.getT1().equals("Kojak")
                        && t.getT2().equals("Lollipops"))
                .expectNextMatches(t -> t.getT1().equals("Barbossa")
                        && t.getT2().equals("Apple"))
                .verifyComplete();
    }

    @Test
    public void zipFluxesToString() {
        Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa");
        Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apple");

        Flux<String> zippedFlux = Flux
                .zip(characterFlux, foodFlux, (cf, ff) -> String.format("%s eats %s", cf, ff));

        StepVerifier.create(zippedFlux)
                .expectNext("Garfield eats Lasagna")
                .expectNext("Kojak eats Lollipops")
                .expectNext("Barbossa eats Apple")
                .verifyComplete();
    }

    @Test
    public void firstWithSignalFlux() {
        Flux<String> slowFlux = Flux.just("tortoise", "snail", "sloth");
        Flux<String> fastFlux = Flux.just("hare", "cheetah", "squirrel");

        Flux<String> firstFlux = Flux.firstWithSignal(fastFlux, slowFlux);

        StepVerifier.create(firstFlux)
                .expectNext("hare")
                .expectNext("cheetah")
                .expectNext("squirrel")
                .verifyComplete();
    }

    @Test
    public void skipFew() {
        Flux<String> countFlux = Flux
                .just("one", "two", "skip a few", "ninety nine", "one hundred")
                .skip(3);

        StepVerifier.create(countFlux)
                .expectNext("ninety nine", "one hundred")
                .verifyComplete();
    }

    @Test
    public void skipFewSeconds() {
        Flux<String> countFlux = Flux
                .just("one", "two", "skip a few", "ninety nine", "one hundred")
                .delayElements(Duration.ofSeconds(1))
                .skip(Duration.ofSeconds(4));

        StepVerifier.create(countFlux)
                .expectNext("ninety nine", "one hundred")
                .verifyComplete();
    }

    @Test
    public void takeForAwhile() {
        Flux<String> nationalParkFlux = Flux
                .just("Yellowstone", "Yosemite", "Grand Canyon", "Zion", "Grand Teton")
                .delayElements(Duration.ofSeconds(1))
                .take(Duration.ofMillis(3500));

        StepVerifier.create(nationalParkFlux)
                .expectNext("Yellowstone", "Yosemite", "Grand Canyon")
                .verifyComplete();
    }

    @Test
    public void filter() {
        Flux<String> nationalParkFlux = Flux
                .just("Yellowstone", "Yosemite", "Grand Canyon", "Zion", "Grand Teton")
                .filter(np -> np.split(" ").length == 1);

        StepVerifier.create(nationalParkFlux)
                .expectNext("Yellowstone", "Yosemite", "Zion")
                .verifyComplete();
    }

    @Test
    public void distinct() {
        Flux<String> animalFlux = Flux
                .just("dog", "cat", "bird", "dog", "bird", "anteater")
                .distinct();

        StepVerifier.create(animalFlux)
                .expectNext("dog", "cat", "bird", "anteater")
                .verifyComplete();
    }

    @Test
    public void map() {
        Flux<Player> playerFlux = Flux
                .just("Michael", "Scottle", "Steve")
                .map(Player::new);

        StepVerifier.create(playerFlux)
                .expectNext(new Player("Michael"))
                .expectNext(new Player("Scottle"))
                .expectNext(new Player("Steve"))
                .verifyComplete();
    }

    @Test
    public void flatMap() {
        Flux<Player> playerFlux = Flux
                .just("Michael", "Scottie", "Steve")
                .flatMap(name -> Mono.just(name)
                        .map(Player::new)
                        .subscribeOn(Schedulers.parallel()));

        List<Player> players = Arrays.asList(
                new Player("Michael"),
                new Player("Scottie"),
                new Player("Steve")
        );

        StepVerifier.create(playerFlux)
                .expectNextMatches(player -> players.contains(player))
                .expectNextMatches(player -> players.contains(player))
                .expectNextMatches(player -> players.contains(player))
                .verifyComplete();
    }

    @Test
    public void buffer() {
        Flux<String> fruitFlux = Flux.just(
                "apple", "orange", "banana", "kiwi", "strawberry");

        Flux<List<String>> bufferedFlux = fruitFlux.buffer(3);

        StepVerifier.create(bufferedFlux)
                .expectNext(Arrays.asList("apple", "orange", "banana"))
                .expectNext(Arrays.asList("kiwi", "strawberry"))
                .verifyComplete();
    }

    @Test
    public void bufferAndFlatMap() {
        Flux<String> fruitFlux = Flux.just(
                "apple", "orange", "banana", "kiwi", "strawberry");

        Flux<String> fruitUpperFlux = fruitFlux
                .buffer(2)
                .flatMap(list -> Flux
                        .fromIterable(list)
                        .map(String::toUpperCase)
                        .subscribeOn(Schedulers.parallel())
                        .log());

        List<String> upperCaseFruits = Arrays.asList("APPLE", "ORANGE", "BANANA", "KIWI", "STRAWBERRY");
        StepVerifier.create(fruitUpperFlux)
                .expectNextMatches(fruit -> upperCaseFruits.contains(fruit))
                .expectNextMatches(fruit -> upperCaseFruits.contains(fruit))
                .expectNextMatches(fruit -> upperCaseFruits.contains(fruit))
                .expectNextMatches(fruit -> upperCaseFruits.contains(fruit))
                .expectNextMatches(fruit -> upperCaseFruits.contains(fruit))
                .verifyComplete();
    }

    @Test
    public void collectList() {
        Flux<String> fruitFlux = Flux.just(
                "apple", "orange", "banana", "kiwi", "strawberry");

        StepVerifier.create(fruitFlux.collectList())
                .expectNext(Arrays.asList("apple", "orange", "banana", "kiwi", "strawberry"))
                .verifyComplete();
    }

    @Test
    public void collectMap() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");

        StepVerifier.create(animalFlux.collectMultimap(animal -> animal.charAt(0)))
                .expectNextMatches(characterStringMap -> characterStringMap.size() == 3 &&
                        characterStringMap.get('a').equals(Collections.singletonList("aardvark")) &&
                        characterStringMap.get('e').equals(Arrays.asList("elephant", "eagle")) &&
                        characterStringMap.get('k').equals(Arrays.asList("koala", "kangaroo")))
                .verifyComplete();
    }

    @Test
    public void all() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");

        Mono<Boolean> hasAMono = animalFlux.all(animal -> animal.contains("a"));
        StepVerifier.create(hasAMono)
                .expectNext(true)
                .verifyComplete();

        Mono<Boolean> hasKMono = animalFlux.all(animal -> animal.contains("k"));
        StepVerifier.create(hasKMono)
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    public void any() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");

        Mono<Boolean> hasTMono = animalFlux.any(animal -> animal.contains("t"));
        StepVerifier.create(hasTMono)
                .expectNext(true)
                .verifyComplete();

        Mono<Boolean> hasZMono = animalFlux.any(animal -> animal.contains("z"));
        StepVerifier.create(hasZMono)
                .expectNext(false)
                .verifyComplete();
    }
}
