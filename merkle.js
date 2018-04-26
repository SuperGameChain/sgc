"use strict";
var crypto = require('crypto');

function hash(str){
	return crypto.createHash("sha256").update(str, "utf8").digest("base64");
}

function getMerkleRoot(arrElements){
	var arrHashes = arrElements.map(hash);
	while (arrHashes.length > 1){
		var arrOverHashes = [];
		for (var i=0; i<arrHashes.length; i+=2){
			var hash2_index = (i+1 < arrHashes.length) ? (i+1) : i;
			arrOverHashes.push(hash(arrHashes[i] + arrHashes[hash2_index]));
		}
		arrHashes = arrOverHashes;
	}
	return arrHashes[0];
}

function getMerkleProof(arrElements, element_index){
	if (index < 0 || index >= arrElements.length)
		throw Error("invalid index");
	var arrHashes = arrElements.map(hash);
	var index = element_index;
	var arrSiblings = [];
	while (arrHashes.length > 1){
		var arrOverHashes = [];
		var overIndex = null;
		for (var i=0; i<arrHashes.length; i+=2){
			var hash2_index = (i+1 < arrHashes.length) ? (i+1) : i;
			if (i === index){
				arrSiblings.push(arrHashes[hash2_index]);
				overIndex = i/2;
			}
			else if (hash2_index === index){
				arrSiblings.push(arrHashes[i]);
				overIndex = i/2;
			}
			arrOverHashes.push(hash(arrHashes[i] + arrHashes[hash2_index]));
		}
		arrHashes = arrOverHashes;
		if (overIndex === null)
			throw Error("overIndex not defined");
		index = overIndex;
	}
	return {
		root: arrHashes[0],
		siblings: arrSiblings,
		index: element_index
	};
}

function serializeMerkleProof(proof){
	var serialized_proof = proof.index;
	if (proof.siblings.length > 0)
		serialized_proof += "-"+proof.siblings.join("-");
	serialized_proof += "-"+proof.root;
	return serialized_proof;
}

function deserializeMerkleProof(serialized_proof){
	var arr = serialized_proof.split("-");
	var proof = {};
	proof.root = arr.pop();
	proof.index = arr.shift();
	proof.siblings = arr;
	return proof;
}

function verifyMerkleProof(element, proof){
	var index = proof.index;
	var the_other_sibling = hash(element);
	for (var i=0; i<proof.siblings.length; i++){
		if (index % 2 === 0)
			the_other_sibling = hash(the_other_sibling + proof.siblings[i]);
		else
			the_other_sibling = hash(proof.siblings[i] + the_other_sibling);
		index = Math.floor(index/2);
	}
	return (the_other_sibling === proof.root);
}

exports.getMerkleRoot = getMerkleRoot;

exports.getMerkleProof = getMerkleProof;
exports.verifyMerkleProof = verifyMerkleProof;

exports.serializeMerkleProof = serializeMerkleProof;
exports.deserializeMerkleProof = deserializeMerkleProof;