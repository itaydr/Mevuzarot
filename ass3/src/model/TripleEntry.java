package model;

import java.util.ArrayList;

public class TripleEntry {
	
	public String path;
	public ArrayList<TripleSlotEntry> slotXs;
	public ArrayList<TripleSlotEntry> slotYs;
	
	public TripleEntry(String path) {
		super();
		
		this.path = path;
		this.slotXs = new ArrayList<TripleSlotEntry>();
		this.slotYs = new ArrayList<TripleSlotEntry>();
	}
	
	public void addSlotX(TripleSlotEntry slot) {
		this.slotXs.add(slot);
	}
	
	public void addSlotY(TripleSlotEntry slot) {
		this.slotYs.add(slot);
	}
}
