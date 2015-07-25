package Utils;

public class DLogger {
	
	private boolean enabled;
		
	public DLogger(boolean enabled2) {
		this.enabled = enabled2;
	}

	public void log(String logStr) {
		if (this.enabled) {
			System.out.println(logStr);
		}
	}
	
	public void log(String logStr, boolean forceLog) {
		if (this.enabled || forceLog) {
			System.out.println(logStr);
		}
	}
}
