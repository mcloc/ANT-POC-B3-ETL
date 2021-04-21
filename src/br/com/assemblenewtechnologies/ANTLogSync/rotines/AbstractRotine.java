package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;

abstract class AbstractRotine implements RotineInterface {

	protected AbstractRotine runnable;
	private boolean executing = false;
	protected MainController mainController;
	/**
	 * @return the mainController
	 */
	public MainController getMainController() {
		return mainController;
	}
	/**
	 * @param mainController the mainController to set
	 */
	public void setMainController(MainController mainController) {
		this.mainController = mainController;
	}
	/**
	 * @return the executing
	 */
	public boolean isExecuting() {
		return executing;
	}
	/**
	 * @param executing the executing to set
	 */
	public void setExecuting(boolean executing) {
		this.executing = executing;
	}
	
	
	
	
	
}
