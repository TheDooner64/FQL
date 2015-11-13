var fs = require('fs');

// _readTable takes a string representing a table name
// and returns an array of objects, namely the rows.
// It does so by looking up actual files, reading them,
// and parsing them from JSON strings into JS objects.
function _readTable (tableName) {
	var folderName = __dirname + '/film-database/' + tableName;
	var fileNames = fs.readdirSync(folderName);
	var fileStrings = fileNames.map(function (fileName) {
		var filePath = folderName + '/' + fileName;
		return fs.readFileSync(filePath).toString();
	});
	var table = fileStrings.map(function (fileStr) {
		return JSON.parse(fileStr);
	});
	return table;
}

function merge (obj1, obj2) {
	var mergedObj = {};

	Object.keys(obj1).forEach(function(key) {
		mergedObj[key] = obj1[key];
	});

	Object.keys(obj2).forEach(function(key) {
		mergedObj[key] = obj2[key];
	});

	return mergedObj;
}

function FQL (table) {
	this.table = table;
	this.indexTables = {};
}


FQL.prototype.exec = function(){
	return this.table;
};


FQL.prototype.count = function(){
	return this.table.length;
};


FQL.prototype.limit = function(num) {
	var limitedTable = this.table.slice(0, num);
	return new FQL(limitedTable);
};

/*
NOTE: This is the original WHERE, which doesn't use the index we built
FQL.prototype.where = function(obj){
	var keys = Object.keys(obj);
	
	// Use "filter" to return new table of elements that pass criteria
	var resultsTable = this.table.filter(function(row) {
		// Use "every" to make sure each key's alue matches
		return keys.every(function(key) {
			if (typeof obj[key] === 'function') {
				return obj[key](row[key]);
			} else {
				return obj[key] === row[key];
			}
		});
	});

	return new FQL(resultsTable);
};
*/

FQL.prototype.where = function(queryObj) {
	var newTable = this.table;
	var self = this;
	Object.keys(queryObj).forEach(function(field) {
		var indices = self.getIndicesOf(field, queryObj[field]);
		// if there are indices I have an index on field 
		if(indices) {
			// retrieve by index super fast!
			newTable = indices.map(function(rowIdx) {
				return self.table[rowIdx];
			});
		}
		// if not, then I must scan the entire table
		else {
			newTable = newTable.filter(function(row) {
				if (typeof queryObj[field] === 'function') {
					if (!queryObj[field](row[field])) {
						return false;
					}
				}
				else if (row[field] !== queryObj[field]){
					return false;
				}
				return true;
			});
		}
	});

	return new FQL(newTable);
};

FQL.prototype.select = function(columnNames) {
	var selectedData = this.table.map(function(row) {
		return columnNames.reduce(function(newRow, colName) {
			newRow[colName] = row[colName];
			return newRow;
		}, {});
	});

	return new FQL(selectedData);
};


FQL.prototype.order = function(colName){
	var sortedTable = this.table.sort(function(a, b) {
		if (typeof a[colName] === "number") {
			return a[colName] - b[colName];
		} else if (a[colName].toLowerCase() < b[colName].toLowerCase()) {
			return -1;
		} else if (a[colName].toLowerCase() > b[colName].toLowerCase()) {
			return 1;
		} else {
			return 0;
		}
	});

	return new FQL(sortedTable);
};


FQL.prototype.left_join = function(otherTableToJoin, conditional) {
	var mergedTable = [];

	this.table.forEach(function(row) {
		otherTableToJoin.table.forEach(function(otherRow) {
			if (conditional(row, otherRow)) {
				var merged = merge(row, otherRow);
				mergedTable.push(merged);
			}
		});
	});

	return new FQL(mergedTable);
};

FQL.prototype.addIndex = function(colName) {
	var index = {};

	// var sortedTable = this.order(colName).table;
	var sortedTable = this.table;

	sortedTable.forEach(function(row, rowIndex) {
		var val = row[colName];
		if (index[val]) {
			index[val].push(rowIndex);
		} else {
			index[val] = [rowIndex];
		}
	});

	this.indexTables[colName] = index;
};

FQL.prototype.getIndicesOf = function(colName, val) {
	if (this.indexTables[colName]) {
		return this.indexTables[colName][val];
	}
};

module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};