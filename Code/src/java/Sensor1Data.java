package org.thingsboard.samples.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sensor1Data implements Serializable {
    private String id;
    private Double value;
}
