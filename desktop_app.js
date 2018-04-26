"use strict";
var fs = require('fs'+'');
var path = require('path'+'');

function getAppsDataDir(){
	switch(process.platform){
		case 'win32': return process.env.LOCALAPPDATA;
		case 'linux': return process.env.HOME + '/.config';
		case 'darwin': return process.env.HOME + '/Library/Application Support';
		default: throw Error("unknown platform "+process.platform);
	}
}

function getPackageJsonDir(start_dir){
	try{
		fs.accessSync(start_dir + '/package.json');
		return start_dir;
	}
	catch(e){
		var parent_dir = path.dirname(start_dir);
		if (parent_dir === '/' || process.platform === 'win32' && parent_dir.match(/^\w:[\/\\]/))
			throw Error('no package.json found');
		return getPackageJsonDir(parent_dir);
	}
}

function getAppRootDir(){
	var mainModuleDir = path.dirname(process.mainModule.paths[0]);
	return getPackageJsonDir(mainModuleDir);
}

function getAppName(){
	var appDir = getAppRootDir();
	console.log("app dir "+appDir);
	return require(appDir + '/package.json').name;
}

function getAppDataDir(){
	return (getAppsDataDir() + '/' + getAppName());
}

exports.getAppRootDir = getAppRootDir;
exports.getAppDataDir = getAppDataDir;