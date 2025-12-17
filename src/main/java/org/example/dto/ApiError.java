package org.example.dto;

// Dto for error payload having status code,message,and details returned by the api
public class ApiError {
    public int code;
    public String message;
    public String detail;

    public ApiError() {}

    public ApiError(int code, String message, String detail) {
        this.code = code;
        this.message = message;
        this.detail = detail;
    }
}
