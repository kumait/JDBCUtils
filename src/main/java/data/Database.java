package data;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by kumait on 12/1/14.
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface Database {
    String column() default "?";
}
