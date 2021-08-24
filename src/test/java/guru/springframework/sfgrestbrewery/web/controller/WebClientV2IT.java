package guru.springframework.sfgrestbrewery.web.controller;

import guru.springframework.sfgrestbrewery.bootstrap.BeerLoader;
import guru.springframework.sfgrestbrewery.web.model.BeerDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static guru.springframework.sfgrestbrewery.web.functional.BeerRouterConfig.BEER_V2_URL;
import static guru.springframework.sfgrestbrewery.web.functional.BeerRouterConfig.BEER_V2_URL_UPC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class WebClientV2IT {

    public static final String BASE_URL = "http://localhost:8080";
    //public static final String BEER_V2_PATH = "api/v2/beer";

    private WebClient webClient;

    @BeforeEach
    void setUp() {
        webClient = WebClient.builder()
                .baseUrl(BASE_URL)
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create().wiretap(true)))
                .build();
    }

    @Test
    void getBeerById() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BEER_V2_URL + "/" + 1)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {
            assertThat(beer).isNotNull();
            assertThat(beer.getBeerName()).isNotNull();

            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByIdNotFound() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BEER_V2_URL + "/" + 1333)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {

        }, throwable -> {
            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByUpc() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BEER_V2_URL_UPC + "/" + BeerLoader.BEER_2_UPC)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {
            assertThat(beer).isNotNull();
            assertThat(beer.getBeerName()).isNotNull();

            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByUpcNoFound() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BEER_V2_URL_UPC + "/44444466677766")
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {

        }, throwable -> {
            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testSaveBeer() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        BeerDto beerDto = BeerDto.builder()
                .beerName("JTs Beer")
                .upc("1233455")
                .beerStyle("PALE_ALE")
                .price(new BigDecimal("8.99"))
                .build();

        Mono<ResponseEntity<Void>> beerResponseMono = webClient.post().uri(BEER_V2_URL)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(beerDto))
                .retrieve().toBodilessEntity();

        beerResponseMono.publishOn(Schedulers.parallel()).subscribe(responseEntity -> {

            assertThat(responseEntity.getStatusCode().is2xxSuccessful());

            countDownLatch.countDown();
        });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testSaveBeerBadRequest() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        BeerDto beerDto = BeerDto.builder()
                .price(new BigDecimal("8.99"))
                .build();

        Mono<ResponseEntity<Void>> beerResponseMono = webClient.post().uri(BEER_V2_URL)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(beerDto))
                .retrieve().toBodilessEntity();

        beerResponseMono.publishOn(Schedulers.parallel()).doOnError(throwable -> {
            countDownLatch.countDown();
        }).subscribe(voidResponseEntity -> {

        });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testUpdateBeer() throws InterruptedException {

        final String newBeerName = "JTs beer";
        final Integer beerId = 1;
        CountDownLatch countDownLatch = new CountDownLatch(2);

        webClient.put().uri(BEER_V2_URL + "/" + beerId)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters
                .fromValue(BeerDto.builder()
                        .beerName(newBeerName)
                        .upc("1233455")
                        .beerStyle("PALE_ALE")
                        .price(new BigDecimal("8.99"))
                        .build()))
                .retrieve().toBodilessEntity()
                .subscribe(responseEntity -> {
                    assertThat(responseEntity.getStatusCode().is2xxSuccessful());
                    countDownLatch.countDown();
                });

        //await for update thread to complete
        countDownLatch.await(500, TimeUnit.MILLISECONDS);

        webClient.get().uri(BEER_V2_URL + "/" + beerId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class)
                .subscribe(beer -> {
                    assertThat(beer).isNotNull();
                    assertThat(beer.getBeerName()).isNotNull();
                    assertThat(beer.getBeerName()).isEqualTo(newBeerName);

                    countDownLatch.countDown();
                });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testUpdateBeerNotFound() throws InterruptedException {

        final String newBeerName = "JTs beer";
        final Integer beerId = 999;
        CountDownLatch countDownLatch = new CountDownLatch(1);

        webClient.put().uri(BEER_V2_URL + "/" + beerId)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters
                .fromValue(BeerDto.builder()
                        .beerName(newBeerName)
                        .upc("1233455")
                        .beerStyle("PALE_ALE")
                        .price(new BigDecimal("8.99"))
                        .build()))
                .retrieve().toBodilessEntity()
                .subscribe(responseEntity -> {
                    assertThat(responseEntity.getStatusCode().is2xxSuccessful());
                }, throwable -> {
                    countDownLatch.countDown();
                });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testDeleteBeer() {
        Integer beerId = 3;
        CountDownLatch countDownLatch = new CountDownLatch(1);

        webClient.delete().uri(BEER_V2_URL + "/" + beerId)
                .retrieve().toBodilessEntity()
                .flatMap(responseEntity -> {
                    countDownLatch.countDown();

                    return webClient.get().uri(BEER_V2_URL + "/" + beerId)
                            .accept(MediaType.APPLICATION_JSON)
                            .retrieve().bodyToMono(BeerDto.class);
                }).subscribe(beerDto -> {

        }, throwable -> {
           countDownLatch.countDown();
        });

    }

    @Test
    void testDeleteBeerNotFound() {
        Integer beerId = 4;

        webClient.delete().uri(BEER_V2_URL + "/" + beerId)
                .retrieve().toBodilessEntity().block();

        assertThrows(WebClientResponseException.NotFound.class, () -> {
           webClient.delete().uri(BEER_V2_URL + "/" + beerId)
                   .retrieve().toBodilessEntity().block();
        });
    }

}
