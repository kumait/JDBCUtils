package data;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by Kumait on 2/14/2016.
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface Database {
    String column() default "?";
}