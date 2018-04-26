"use strict";
var async = require('async');
var storage = require('./storage.js');
var myWitnesses = require('./my_witnesses.js');
var objectHash = require("./object_hash.js");
var db = require('./db.js');
var constants = require("./constants.js");
var validation = require('./validation.js');

function prepareWitnessProof(arrWitnesses, last_stable_mci, handleResult){
	var arrWitnessChangeAndDefinitionJoints = [];
	var arrUnstableMcJoints = [];
	
	var arrLastBallUnits = [];
	var last_ball_unit = null;
	var last_ball_mci = null;
	
	async.series([
		function(cb){
			storage.determineIfWitnessAddressDefinitionsHaveReferences(db, arrWitnesses, function(bWithReferences){
				bWithReferences ? cb("some witnesses have references in their addresses, please change your witness list") : cb();
			});
		},
		function(cb){
			var arrFoundWitnesses = [];
			db.query(
				"SELECT unit FROM units WHERE is_on_main_chain=1 AND is_stable=0 ORDER BY main_chain_index DESC", 
				function(rows){
					async.eachSeries(rows, function(row, cb2){
						storage.readJointWithBall(db, row.unit, function(objJoint){
							delete objJoint.ball;
							arrUnstableMcJoints.push(objJoint);
							for (var i=0; i<objJoint.unit.authors.length; i++){
								var address = objJoint.unit.authors[i].address;
								if (arrWitnesses.indexOf(address) >= 0 && arrFoundWitnesses.indexOf(address) === -1)
									arrFoundWitnesses.push(address);
							}
							if (objJoint.unit.last_ball_unit && arrFoundWitnesses.length >= constants.MAJORITY_OF_WITNESSES)
								arrLastBallUnits.push(objJoint.unit.last_ball_unit);
							cb2();
						});
					}, cb);
				}
			);
		},
		function(cb){
			if (arrLastBallUnits.length === 0)
				return cb("your witness list might be too much off, too few witness authored units");
			db.query("SELECT unit, main_chain_index FROM units WHERE unit IN(?) ORDER BY main_chain_index DESC LIMIT 1", [arrLastBallUnits], function(rows){
				last_ball_unit = rows[0].unit;
				last_ball_mci = rows[0].main_chain_index;
				(last_stable_mci >= last_ball_mci) ? cb("already_current") : cb();
			});
		},
		function(cb){
			var after_last_stable_mci_cond = (last_stable_mci > 0) ? "latest_included_mc_index>="+last_stable_mci : "1";
			db.query(
				"SELECT unit, `level` \n\
				FROM unit_authors "+db.forceIndex('unitAuthorsIndexByAddressDefinitionChash')+" \n\
				CROSS JOIN units USING(unit) \n\
				WHERE address IN(?) AND definition_chash IS NOT NULL AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
				UNION \n\
				SELECT unit, `level` \n\
				FROM address_definition_changes \n\
				CROSS JOIN units USING(unit) \n\
				WHERE address_definition_changes.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
				ORDER BY `level`", 
				[arrWitnesses, arrWitnesses],
				function(rows){
					async.eachSeries(rows, function(row, cb2){
						storage.readJoint(db, row.unit, {
							ifNotFound: function(){
								throw Error("prepareWitnessProof definition changes: not found "+row.unit);
							},
							ifFound: function(objJoint){
								arrWitnessChangeAndDefinitionJoints.push(objJoint);
								cb2();
							}
						});
					}, cb);
				}
			);
		}
	], function(err){
		if (err)
			return handleResult(err);
		handleResult(null, arrUnstableMcJoints, arrWitnessChangeAndDefinitionJoints, last_ball_unit, last_ball_mci);
	});
}

function processWitnessProof(arrUnstableMcJoints, arrWitnessChangeAndDefinitionJoints, bFromCurrent, handleResult){

	myWitnesses.readMyWitnesses(function(arrWitnesses){

		var arrParentUnits = null;
		var arrFoundWitnesses = [];
		var arrLastBallUnits = [];
		var assocLastBallByLastBallUnit = {};
		var arrWitnessJoints = [];
		for (var i=0; i<arrUnstableMcJoints.length; i++){
			var objJoint = arrUnstableMcJoints[i];
			var objUnit = objJoint.unit;
			if (objJoint.ball)
				return handleResult("unstable mc but has ball");
			if (!validation.hasValidHashes(objJoint))
				return handleResult("invalid hash");
			if (arrParentUnits && arrParentUnits.indexOf(objUnit.unit) === -1)
				return handleResult("not in parents");
			var bAddedJoint = false;
			for (var j=0; j<objUnit.authors.length; j++){
				var address = objUnit.authors[j].address;
				if (arrWitnesses.indexOf(address) >= 0){
					if (arrFoundWitnesses.indexOf(address) === -1)
						arrFoundWitnesses.push(address);
					if (!bAddedJoint)
						arrWitnessJoints.push(objJoint);
					bAddedJoint = true;
				}
			}
			arrParentUnits = objUnit.parent_units;
			if (objUnit.last_ball_unit && arrFoundWitnesses.length >= constants.MAJORITY_OF_WITNESSES){
				arrLastBallUnits.push(objUnit.last_ball_unit);
				assocLastBallByLastBallUnit[objUnit.last_ball_unit] = objUnit.last_ball;
			}
		}
		if (arrFoundWitnesses.length < constants.MAJORITY_OF_WITNESSES)
			return handleResult("not enough witnesses");

		
		if (arrLastBallUnits.length === 0)
			throw Error("processWitnessProof: no last ball units");

		for (var i=0; i<arrWitnessChangeAndDefinitionJoints.length; i++){
			var objJoint = arrWitnessChangeAndDefinitionJoints[i];
			var objUnit = objJoint.unit;
			if (!objJoint.ball)
				return handleResult("witness_change_and_definition_joints: joint without ball");
			if (!validation.hasValidHashes(objJoint))
				return handleResult("witness_change_and_definition_joints: invalid hash");
			var bAuthoredByWitness = false;
			for (var j=0; j<objUnit.authors.length; j++){
				var address = objUnit.authors[j].address;
				if (arrWitnesses.indexOf(address) >= 0)
					bAuthoredByWitness = true;
			}
			if (!bAuthoredByWitness)
				return handleResult("not authored by my witness");
		}
		
		var assocDefinitions = {};
		var assocDefinitionChashes = {};
		function validateUnit(objUnit, bRequireDefinitionOrChange, cb2){
			var bFound = false;
			async.eachSeries(
				objUnit.authors,
				function(author, cb3){
					var address = author.address;
					if (arrWitnesses.indexOf(address) === -1)
						return cb3();
					var definition_chash = assocDefinitionChashes[address];
					if (!definition_chash)
						throw Error("definition chash not known for address "+address);
					if (author.definition){
						if (objectHash.getChash160(author.definition) !== definition_chash)
							return cb3("definition doesn't hash to the expected value");
						assocDefinitions[definition_chash] = author.definition;
						bFound = true;
					}

					function handleAuthor(){
						validation.validateAuthorSignaturesWithoutReferences(author, objUnit, assocDefinitions[definition_chash], function(err){
							if (err)
								return cb3(err);
							for (var i=0; i<objUnit.messages.length; i++){
								var message = objUnit.messages[i];
								if (message.app === 'address_definition_change' 
										&& (message.payload.address === address || objUnit.authors.length === 1 && objUnit.authors[0].address === address)){
									assocDefinitionChashes[address] = message.payload.definition_chash;
									bFound = true;
								}
							}
							cb3();
						});
					}

					if (assocDefinitions[definition_chash])
						return handleAuthor();
					storage.readDefinition(db, definition_chash, {
						ifFound: function(arrDefinition){
							assocDefinitions[definition_chash] = arrDefinition;
							handleAuthor();
						},
						ifDefinitionNotFound: function(d){
							throw Error("definition "+definition_chash+" not found, address "+address);
						}
					});
				},
				function(err){
					if (err)
						return cb2(err);
					if (bRequireDefinitionOrChange && !bFound)
						return cb2("neither definition nor change");
					cb2();
				}
			);
		}
		
		var unlock = null;
		async.series([
			function(cb){
				if (!bFromCurrent){
					arrWitnesses.forEach(function(address){
						assocDefinitionChashes[address] = address;
					});
					return cb();
				}
				async.eachSeries(
					arrWitnesses, 
					function(address, cb2){
						storage.readDefinitionByAddress(db, address, null, {
							ifFound: function(arrDefinition){
								var definition_chash = objectHash.getChash160(arrDefinition);
								assocDefinitions[definition_chash] = arrDefinition;
								assocDefinitionChashes[address] = definition_chash;
								cb2();
							},
							ifDefinitionNotFound: function(definition_chash){
								assocDefinitionChashes[address] = definition_chash;
								cb2();
							}
						});
					},
					cb
				);
			},
			function(cb){
				async.eachSeries(
					arrWitnessChangeAndDefinitionJoints,
					function(objJoint, cb2){
						var objUnit = objJoint.unit;
						if (!bFromCurrent)
							return validateUnit(objUnit, true, cb2);
						db.query("SELECT 1 FROM units WHERE unit=? AND is_stable=1", [objUnit.unit], function(rows){
							if (rows.length > 0)
								return cb2();
							validateUnit(objUnit, true, cb2);
						});
					},
					cb
				);
			},
			function(cb){
				async.eachSeries(
					arrWitnessJoints.reverse(),
					function(objJoint, cb2){
						validateUnit(objJoint.unit, false, cb2);
					},
					cb
				);
			},
		], function(err){
			err ? handleResult(err) : handleResult(null, arrLastBallUnits, assocLastBallByLastBallUnit);
		});
		
	});

}
exports.prepareWitnessProof = prepareWitnessProof;
exports.processWitnessProof = processWitnessProof;