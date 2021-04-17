package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Core extends Rotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Core.class);
	
	public void startup() throws Exception{
		LOGGER.info("[CORE] startup...");
	}

}
