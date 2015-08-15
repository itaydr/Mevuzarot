package model;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import Utils.Constants;
import Utils.DLogger;


public class NGramFactory {
	
	/**
	 *  Main array  - 
	 *  	head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
	 *  Example - 
	 *  	experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
	 *  Syntactic biarcs NGram -
	 *  	that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0
	 */
	final static int NUMER_OF_OBJECTS_IN_MAIN_ARRAY 	= 3;
	final static int TOTAL_COUNT_INDEX					= 2;
	final static int NGRAM_INDEX 						= 1;
	final static int MIN_SYNTACTIC_NGRAM_LENGTH 		= 3;
	final static DLogger L 								= new DLogger(true, "NGramFactory");
	final static String SYNTACTIC_NGRAM_HEAD_SPLIT_CHAR = "  "; // Two spaces.
	final static String PARSED_WORD_SPLIT_CHAR 			= "/";
	final static String NOUN_PREFIX 					= "NN";
	final static String VERB_PREFIX 					= "VB";
	final static int INDEX_OF_HEAD_WORD					= 0;
			
	private static boolean isNoun(String type) {
		return type.length() > 1 && type.substring(0, 2).equalsIgnoreCase(NOUN_PREFIX);
	}
	
	public static ArrayList<NGram> parse(String input) {
		
		ArrayList<NGram> output = new ArrayList<NGram>();
		StringTokenizer tokenizer = new StringTokenizer(input, "\t");
		tokenizer.nextToken();
		String ngram = tokenizer.nextToken();
		int count = Integer.parseInt(tokenizer.nextToken());
		
		String[] splitted = ngram.toString().trim().split("\\s+");
				
		int root = -1;
		
		int size = 0;
		for (int i = 0; i < splitted.length; i++) {
			String[] subs = splitted[i].toString().trim().split("\\/");
			if (subs.length == 4)
				size++;
		}
		Word[] words = new Word[size+1];
		int index = 1;
		
		for (int i = 1; i <=  splitted.length; i++) {
			String[] subs = splitted[i-1].toString().trim().split("\\/");		
			if (subs.length != 4) {
				return output;
			}
			
			if (subs[3].equals("0")) {
				if (subs[1].length() < 2 || !subs[1].substring(0, 2).equalsIgnoreCase(VERB_PREFIX)) {
					return output;
				}
				root = i;
			}
			
			words[index] =  new Word(subs[0], subs[1], subs[3]);
			index++;
		}
		
		if (root < 0) {
			return output;
		}
		
		for (int i = 1; i < words.length; i++) {
			
			if (!isNoun(words[i].type)) 
				continue;
			
			for (int j = i+1; j < words.length; j++) {
				if (isNoun(words[j].type))  {
					String[] paths = makePaths(i, j, root, words);
					if (paths != null) {

						NGram ng = new NGram(paths[0], words[i].data, words[j].data, count);
						output.add(ng);
						
						ng = new NGram(paths[1], words[j].data, words[i].data, count);
						output.add(ng);
					}
				}
			}
		}
		
		return output;
	}
		
	private static String[] makePaths(int i, int j, int root, Word[] words) throws NumberFormatException {
		String[] pathArr = new String[words.length + 1];
		String[] ans = new String[2];
		int counter = 0;
		
		int index = Integer.parseInt(words[i].node);
		while (index != root) {
			pathArr[index] = words[index].data;
			index = Integer.parseInt(words[index].node);
			counter++;
			if (counter > 15) {
				return null;
			}
		}
		
		if (pathArr[j] != null)
			return null;
		
		counter = 0;
		
		index = Integer.parseInt(words[j].node);
		while (index != root) {
			pathArr[index] = words[index].data;
			index = Integer.parseInt(words[index].node);
			counter++;
			if (counter > 15) {
				return null;
			}
		}
		
		pathArr[i] = Constants.SLOT_X;
		pathArr[j] = Constants.SLOT_Y;
		pathArr[root] = words[root].data;

		String path = "";
		for (int k = 0; k < pathArr.length; k++) {
			path += (pathArr[k] == null) ? "" : pathArr[k] + " ";
		}
		
		ans[0] = path;
		
		pathArr[i] = Constants.SLOT_Y;
		pathArr[j] = Constants.SLOT_X;
		
		path = "";
		for (int k = 0; k < pathArr.length; k++) {
			path += (pathArr[k] == null) ? "" : pathArr[k] + " ";
		}
		
		ans[1] = path;
		return ans;
	}
	
	
	public static void main(String[] args) {
	String s="begin	first/NN/advmod/2 begin/VB/advcl/0 care/NN/xcomp/2	19	1887,3	1912,2	1968,2	2004,5	2007,2	2008,5";
	
	try {
		//while ((sCurrentLine = br.readLine()) != null) {
			ArrayList<NGram> ngrams = parse(s);
			for (NGram ngram  : ngrams) {
				L.log("Ngram = " + ngram);
			}
		//}
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}
