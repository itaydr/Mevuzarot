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
		
	public static ArrayList<NGram> parseNGram(String ngramStr) {
		
		if (ngramStr == null) {
			L.log("Cannot parse null string.");
			return null;
		}
		
		String[] mainArr = ngramStr.split("	");//("      ");
		
		if (mainArr.length < NUMER_OF_OBJECTS_IN_MAIN_ARRAY) {
			L.log("Less then " + NUMER_OF_OBJECTS_IN_MAIN_ARRAY + " objcts in the NGram array: " + ngramStr);
			return null;
		}
		
		String countStr = mainArr[TOTAL_COUNT_INDEX];
		double count = 0;
		try {
			count = Double.parseDouble(countStr);
		}
		catch (Exception e) {L.log("Failed to parse count: " + countStr + " for ngram: " + ngramStr);}
		
		String syntacticNgram = mainArr[NGRAM_INDEX];

		String[] syntacticNGramArr = syntacticNgram.split(" ");
		
		
		if (syntacticNGramArr.length < MIN_SYNTACTIC_NGRAM_LENGTH) {
			L.log("Too few arguments in syntactic ngram: ." + syntacticNgram);
			return null;
		}
		
		String slotX = null;
		String slotY = null;
		String path = "";
		
		for (int i = 0 ; i < syntacticNGramArr.length ; i++) {
			String parsedWord =  syntacticNGramArr[i];
			String[] parts = parsedWord.trim().split(PARSED_WORD_SPLIT_CHAR);
			if (parts.length < 4) {
				continue;
			}
			String word = parts[0];
			String type = parts[1];
			int index = -1;
			try {
				index = Integer.parseInt(parts[3]);
			}catch(Exception e) {L.log("Failed to parse word " + parsedWord);}
			
			if (index == -1) {
				continue;
			}
			// Root
			else if (index == 0) {
				if (!type.startsWith(VERB_PREFIX)) {
					// All roots must be verbs!
					return null;
				}
			}
			
			boolean isNoun = type.startsWith(NOUN_PREFIX);
			if (slotX == null && isNoun) {
				slotX = word;
				continue;
			}
			else if (slotY == null && isNoun) {
				slotY = word;
				continue;
			}
			else if (path.length() > 0) {
				path += " ";
			}
			
			path += word;
		}
		
		if (slotX == null || slotY == null || path == null || count == 0) {
			//L.log("Failed to parse, one of the objects was not proper: " + ngramStr);
			return null;
		}
		
		NGram ngram1 = new NGram(Constants.SLOT_X + Constants.SPACE + path + Constants.SPACE + Constants.SLOT_Y, slotX, slotY, count);
		//NGram ngram2 = new NGram(Constants.SLOT_Y + Constants.SPACE + path + Constants.SPACE + Constants.SLOT_X, slotY, slotX, count);
		ArrayList<NGram> result = new ArrayList<NGram>();
		result.add(ngram1);
		//result[1] = ngram2;
		
		return result;
	}
	
	private static boolean isNoun(String type) {
		return type.length() > 1 && type.substring(0, 2).equalsIgnoreCase("NN");
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
		String[][] words = new String[size+1][3];
		int index = 1;
		
		for (int i = 1; i <=  splitted.length; i++) {
			String[] subs = splitted[i-1].toString().trim().split("\\/");		
			if (subs.length != 4) {
				return output;
			}
			
			if (subs[3].equals("0")) {
				if (subs[1].length() < 2 || !subs[1].substring(0, 2).equalsIgnoreCase("VB")) {
					return output;
				}
				root = i;
			}
			
			words[index][0] = subs[0];
			words[index][1] = subs[1];
			words[index][2] = subs[3];
			index++;
		}
		
		if (root < 0) {
			return output;
		}
		
		for (int i = 1; i < words.length; i++) {
			
			if (!isNoun(words[i][1])) 
				continue;
			
			for (int j = i+1; j < words.length; j++) {
				if (isNoun(words[j][1]))  {
					String[] paths = makePaths(i, j, root, words);
					if (paths != null) {

						NGram ng = new NGram(paths[0], words[i][0], words[j][0], count);
						output.add(ng);
						
						ng = new NGram(paths[1], words[j][0], words[i][0], count);
						output.add(ng);
					}
				}
			}
		}
		
		return output;
	}
		
	private static String[] makePaths(int i, int j, int root, String[][] words) throws NumberFormatException {
		String[] pathArr = new String[words.length + 1];
		String[] ans = new String[2];
		int counter = 0;
		
		int index = Integer.parseInt(words[i][2]);
		while (index != root) {
			pathArr[index] = words[index][0];
			index = Integer.parseInt(words[index][2]);
			counter++;
			if (counter > 15) {
				return null;
			}
		}
		
		if (pathArr[j] != null)
			return null;
		
		counter = 0;
		
		index = Integer.parseInt(words[j][2]);
		while (index != root) {
			pathArr[index] = words[index][0];
			index = Integer.parseInt(words[index][2]);
			counter++;
			if (counter > 15) {
				return null;
			}
		}
		
		pathArr[i] = Constants.SLOT_X;
		pathArr[j] = Constants.SLOT_Y;
		pathArr[root] = words[root][0];

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
