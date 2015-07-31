package model;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

public class TripleSlotEntry {
	public String word;
	public long count;
	public double mi;
	
	public TripleSlotEntry(String word, long count, double mi) {
		super();
		this.word 	= word;
		this.count 	= count;
		this.mi 	= mi;
	}

	@Override
	public boolean equals(Object obj) {
		// same class, and equal words.
		return obj != null && obj instanceof TripleSlotEntry  && this.word.equals(((TripleSlotEntry)obj).word);
	}
	
	
}
