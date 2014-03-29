package org.eclipse.egit.github.core;

import java.io.Serializable;
import java.util.List;

public class SearchUser  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1823837427834L;

	public int total_count;
	
	public List<User> items;
}
