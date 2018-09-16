package com.model;

import java.util.Date;

import lombok.Data;

@Data
public class Message {	
	private String userId;
	private String profile;
	private Date date;
}
