package entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Map;

@Data
@NoArgsConstructor
//PM2.5 record
public class Record {
    Date date;
    int dewp; //Dew Point (Celsius Degree)
    double humi; //Humidity (%)
    int pres; //Pressure (hPa)
    double temp; //Temperature (Celsius Degree)
}
