package entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
//PM2.5 record
public class Record {
    LocalDateTime time;
    String name;
    int dewp; //Dew Point (Celsius Degree)
    int humi; //Humidity (%)
    int pres; //Pressure (hPa)
    int temp; //Temperature (Celsius Degree)
    int pm; //pm2.5
}
