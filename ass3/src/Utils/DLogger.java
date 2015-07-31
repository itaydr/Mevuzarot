package Utils;

public class DLogger {
	
	private boolean enabled;
	private String TAG;
		
	public DLogger(boolean enabled2, String TAG) {
		this.enabled = enabled2;
		this.TAG = TAG;
	}

	public void log(String logStr) {
		if (this.enabled) {
			System.out.println(TAG + ":" +logStr);
		}
	}
	
	public void log(String logStr, boolean forceLog) {
		if (this.enabled || forceLog) {
			System.out.println(TAG + ":" +logStr);
		}
	}
}
