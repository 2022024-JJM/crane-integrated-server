package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.application.port.in.InvalidQueryException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 도메인 예외 → RFC 9457 ProblemDetail. 표준 예외(파라미터 누락·형식 오류 등)는
 * spring.mvc.problemdetails.enabled=true 가 처리한다. 여기서는 로그를 남기지 않는다(DB 장애 로그는 컨트롤러가 남긴다).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(CraneNotFoundException.class)
    ProblemDetail craneNotFound(CraneNotFoundException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
        problem.setTitle("Crane not found");
        problem.setProperty("craneId", e.craneId());
        return problem;
    }

    @ExceptionHandler(InvalidQueryException.class)
    ProblemDetail invalidQuery(InvalidQueryException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Invalid query");
        return problem;
    }

    @ExceptionHandler(HistoryUnavailableException.class)
    ProblemDetail historyUnavailable(HistoryUnavailableException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "History database is not reachable, try again later");
        problem.setTitle("History unavailable");
        return problem;
    }
}
