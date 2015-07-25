package model;

public class NGram {
	public final String path;
	public final String slotX;
	public final String slotY;
	public final double count;
	
	public NGram(String path, String slotX, String slotY, double count) {
		super();
		this.path 	= path;
		this.slotX 	= slotX;
		this.slotY 	= slotY;
		this.count 	= count;
	}
}
