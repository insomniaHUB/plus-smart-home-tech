package ru.yandex.practicum.gateway.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(GatewaySecurityTest.MockGatewayRoutesConfig.class)
class GatewaySecurityTest {


    @Autowired
    private WebTestClient webTestClient;

    @TestConfiguration
    static class MockGatewayRoutesConfig {

        @Bean
        public RouteLocator testRouteLocator(RouteLocatorBuilder builder) {
            return builder.routes()
                    .route("test-products", r -> r.path("/api/products/**", "/api/categories/**", "/api/inventory/**")
                            .filters(f -> f.setPath("/mock-backend"))
                            .uri("forward:///"))
                    .route("test-orders", r -> r.path("/api/orders/**")
                            .filters(f -> f.setPath("/mock-backend"))
                            .uri("forward:///"))
                    .build();
        }

        @RestController
        static class DummyBackendController {
            @RequestMapping("/mock-backend")
            public Mono<ResponseEntity<String>> handle() {
                return Mono.just(ResponseEntity.ok("OK"));
            }
        }
    }

    @Test
    void publicGetProducts_withoutAuth_shouldReturnOk() {
        webTestClient.get()
                .uri("/api/products")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void createOrder_withoutAuth_shouldReturnUnauthorized() {
        webTestClient.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void createOrder_withUserRoleIvan_shouldReturnOk() {
        webTestClient.post()
                .uri("/api/orders")
                .headers(headers -> headers.setBasicAuth("ivan", "ivan"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void adminOperations_withUserRoleIvan_shouldReturnForbidden() {
        webTestClient.post()
                .uri("/api/products")
                .headers(headers -> headers.setBasicAuth("ivan", "ivan"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isForbidden();

        webTestClient.get()
                .uri("/api/orders")
                .headers(headers -> headers.setBasicAuth("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void adminOperations_withAdminRoleAnna_shouldReturnOk() {
        webTestClient.post()
                .uri("/api/products")
                .headers(headers -> headers.setBasicAuth("anna", "anna"))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();

        webTestClient.get()
                .uri("/api/orders")
                .headers(headers -> headers.setBasicAuth("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void preflightOptionsRequest_withoutAuth_shouldReturnCorsHeaders() {
        webTestClient.options()
                .uri("/api/orders")
                .header(HttpHeaders.ORIGIN, "http://localhost:3000")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, HttpMethod.POST.name())
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS, HttpHeaders.AUTHORIZATION)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:3000")
                .expectHeader().value(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS,
                        methods -> assertThat(methods).contains("POST"))
                .expectHeader().value(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS,
                        headers -> assertThat(headers.toLowerCase()).contains("authorization"));
    }


    @Test
    void unknownRoute_evenWithAdminAnna_shouldBeDenied() {
        webTestClient.get()
                .uri("/api/unknown-secret-endpoint")
                .headers(headers -> headers.setBasicAuth("anna", "anna"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void userDetailsService_whenNoUsersConfigured_shouldThrowException() {
        GatewaySecurityConfig config = new GatewaySecurityConfig(new SecurityProperties());

        assertThatThrownBy(config::userDetailsService)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("app.security.users");
    }
}
