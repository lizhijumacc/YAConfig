package com.yaconfig.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MySQLValue {
	String connStr();

	String keyName();
	
	String valueName();

	String tableName();
	
	String key();
	
	String userName();
	
	String password();
}