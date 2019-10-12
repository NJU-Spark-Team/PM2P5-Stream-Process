package entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


/**
 * data entity for pn2.5 record
 *
 * @author Nosolution
 * @version 1.0
 * @since 2019/10/10
 */
@Data
@NoArgsConstructor
public class Record {
    LocalDateTime time;
    String name;
    int dewp; //Dew Point (Celsius Degree)
    int humi; //Humidity (%)
    int pres; //Pressure (hPa)
    int temp; //Temperature (Celsius Degree)
    int pm; //pm2.5
}
