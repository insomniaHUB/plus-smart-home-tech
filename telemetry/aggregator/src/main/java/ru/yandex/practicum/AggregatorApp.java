package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.handler.SensorEventHandler;

@SpringBootApplication
public class AggregatorApp {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApp.class, args);
        SensorEventHandler handler = context.getBean(SensorEventHandler.class);
        handler.start();
    }
}