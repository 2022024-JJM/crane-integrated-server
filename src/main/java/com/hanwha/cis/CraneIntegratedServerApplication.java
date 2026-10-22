package com.hanwha.cis;

import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CraneIntegratedServerApplication {

    public static void main(String[] args) {
        // 처리되지 않은 예외로 스레드가 죽으면 기본 동작은 stderr 출력뿐이라 로그 파일에 남지 않는다.
        Thread.setDefaultUncaughtExceptionHandler((thread, e) -> LoggerFactory
                .getLogger(CraneIntegratedServerApplication.class)
                .error("Thread {} died with an uncaught exception", thread.getName(), e));
        SpringApplication.run(CraneIntegratedServerApplication.class, args);
    }

}
