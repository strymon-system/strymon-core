fn is_parent(v1: &Vec<u32>, v2: &Vec<u32>) -> bool {
	if v1.len()<(v2.len()+1) && v1[..] == v2[..v2.len()-1] {
		return true;
	}
	return false;
}

// Method to convert a Vec<Vec<u32>> indicating paths through a tree to a canonical representation of the tree

// the result is a sequence of degrees of a BFS traversal of the graph.
pub fn reconstruct(paths: &Vec<Vec<u32>>) -> Vec<u32> {
	let mut position = vec![0; paths.len()];
	let mut degrees = vec![0];
	let mut offsets = vec![1]; // where do children start?

	let mut offset_pos = 2;

	if let Some(max_depth) = paths.iter().map(|p| p.len()).max() {
		for depth in 0 .. max_depth {
			// advance each position based on its offset
			// ensure that the max degree of the associated node is at least as high as it should be.
			for index in 0..paths.len() {
				if paths[index].len() > depth {
					if depth > 0 {
						position[index] = (offsets[position[index]] + paths[index][depth-1]) as usize;
					}

					degrees[position[index]] = ::std::cmp::max(degrees[position[index]], paths[index][depth] + 1);
				}
			}

			// add zeros and transform degrees to offsets.
			let mut last = 0;
			for &x in &degrees { last += x as usize; }

			while degrees.len() <= last { 
				degrees.push(0); 
				offsets.push(0); 
			}

			for i in 1..degrees.len() {
				offsets[i] = offsets[i-1] + degrees[i-1];
			}

		}
	}

	return degrees;
}

// extracts pairs of the form "service_1 calls service_2" from all trees in a session
pub fn service_calls(messages: &mut Vec<(Vec<u32>,String,String)>) -> Vec<(String,String)> {
	messages.sort_by(|a,b| a.0.cmp(&b.0));
	let mut pairs = Vec::new();
	for snd in 0..messages.len() {
		let ref a = messages[snd];
		for rcv in snd+1..messages.len(){
			let ref b = messages[rcv];
			if !is_parent(&a.0,&b.0) {break;}
			if a.2==b.2 {// should be a call
				pairs.push((a.1.clone(),b.1.clone()));
			}
		} 
	}
	//println!("Pairs: {:?}",pairs);
	pairs
}


#[test]
fn test_me() {
	assert_eq!(reconstruct(&vec![vec![2, 1, 3], vec![3]]), vec![4,0,0,2,0,0,4,0,0,0,0]);
}
