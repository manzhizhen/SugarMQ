package com.sugarmq.message;

import java.io.Serializable;

import javax.jms.Destination;

public class SugarDestination implements Destination, Serializable {

	private static final long serialVersionUID = 4315929928684782158L;
	
	protected String name;
}
