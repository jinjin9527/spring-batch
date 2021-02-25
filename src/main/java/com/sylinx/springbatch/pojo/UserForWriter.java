package com.sylinx.springbatch.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserForWriter {
    private Long id;
    private String username;
    private String password;
    private int age;
}
