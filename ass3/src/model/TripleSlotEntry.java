package model;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

public class TripleSlotEntry {
	public String word;
	public long count;
	public double mi;
	public double tfidf;
	public double dice;
	
	public TripleSlotEntry(String word, long count, double mi, double tfidf, double dice) {
		super();
		this.word 	= word;
		this.count 	= count;
		this.mi 	= mi;
		this.tfidf 	= tfidf;
		this.dice 	= dice;
	}

	@Override
	public boolean equals(Object obj) {
		// same class, and equal words.
		return obj != null && obj instanceof TripleSlotEntry  && this.word.equals(((TripleSlotEntry)obj).word);
	}

	@Override
	public String toString() {
		return word + ", " + count + ", " + mi + ", " + tfidf + ", " + dice;
	}
	
	
}
