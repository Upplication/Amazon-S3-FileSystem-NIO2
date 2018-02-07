package com.upplication.s3fs.channels.multipart;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Part<T> {

    private PartKey key;
    private int number;
    private T value;

}
