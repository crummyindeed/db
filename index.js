const fs = require('fs');
const path = require('path');
const readline = require('readline');
const stream = require('stream');

const SEP = String.fromCharCode(0x1f);
const DEL = "__DEL__";
const LOGMAX = 4000000;
const SSTMAX = 2000000;

/*
  Utility Functions that make async file operations easier
*/
const ensureDirectory = async function (directory) {
  return new Promise(function (resolve, reject) {
    fs.access(directory, fs.constants.F_OK, async function (err) {
      if (err) {
        fs.mkdirSync(directory);
      }
      resolve(true);
    });
  });
}

const ensureFullPath = async function (directory) {
  var dirs = directory.split(path.sep);
  var full_path = '';
  for (let i in dirs) {
    if (full_path != '') {
      full_path += path.sep;
    }
    full_path += dirs[i];
    await ensureDirectory(full_path);
  }

}

/*
  Interface
*/

const CrummyDB = function (directory) {
  this.directory = directory;
  this.logsize = 0;
}

CrummyDB.prototype.init = async function () {
  this.manifest = await this.getManifest();
  this.memLog = await this.getMemLog();
  this.diskLog = fs.createWriteStream(path.join(this.directory, 'current.log'), { 'flags': 'a' });
  return true;
}

CrummyDB.prototype.put = async function (key, value) {
  value = JSON.stringify(value);
  var insert = key + SEP + value + '\n';
  this.logsize += Buffer.byteLength(insert, 'utf8');
  this.diskLog.write(insert);
  this.memLog[key] = value;
  if (this.logsize >= LOGMAX) {
    await this.newMemLog();
  }
}

CrummyDB.prototype.get = async function (key) {
  var self = this;
  return new Promise(async function (resolve, reject) {
    var mem_val = self.memLog[key];
    if (typeof mem_val != 'undefined') {
      var val = mem_val;
    } else {
      var val = await self.search(key);
    }
    val = JSON.parse(val);
    if (val == DEL) {
      val = false;
    }
    resolve(val);
  });
}

CrummyDB.prototype.delete = async function (key) {
  await this.put(key, DEL);
}
/*
  Background
*/

CrummyDB.prototype.getManifest = async function () {
  var self = this;
  return new Promise(async function (resolve, reject) {
    await ensureFullPath(path.join(self.directory, 'manifest'));
    var list = fs.readdirSync(path.join(self.directory, 'manifest'));
    if (list.length == 0) {
      resolve({});
    } else {
      fs.readFile(path.join(self.directory, 'manifest', list[list.length - 1]), (err, data) => {
        if (err) {
          reject(err);
        } else {
          resolve(JSON.parse(data));
        }
      });
    }
  });
}

CrummyDB.prototype.saveManifest = async function () {
  var self = this;
  return new Promise(function (resolve, reject) {
    var file = path.join(self.directory, 'manifest', Date.now() + '.json');
    var content = JSON.stringify(self.manifest);
    fs.writeFile(file, content, function (err) {
      if (err) {
        reject(err);
      } else {
        resolve(true);
      }
    });
  });
}

CrummyDB.prototype.getMemLog = async function () {
  var self = this;
  return new Promise((resolve, reject) => {
    var log_file = path.join(this.directory, 'current.log');
    fs.access(log_file, fs.constants.F_OK, async function (err) {
      if (err) {
        resolve({});
      } else {
        var stats = fs.statSync(log_file);
        self.logsize = stats["size"];
        var mem_table = await self.fileToMem(log_file);
        resolve(mem_table);
      }
    });
  });
}

CrummyDB.prototype.newMemLog = async function () {
  var self = this;
  var memLog = this.memLog;
  this.memLog = {};
  var time = Date.now();
  var log_name = time + ".log";
  var young_name = time + ".sst";
  fs.renameSync(
    path.join(this.directory, 'current.log'),
    path.join(this.directory, log_name)
  );
  this.diskLog = fs.createWriteStream(path.join(self.directory, 'current.log'), { 'flags': 'a' });
  this.logsize = 0;
  var young_dir = path.join(self.directory, 'young');
  await ensureFullPath(young_dir);
  var files = await self.memToSST(memLog, young_dir);
  for (let i in files) {
    var young_file = files[i].filename;
    var range = files[i].range;
    await self.addFileToManifest('young', young_file, range.min_key, range.max_key);
  }
  if (self.manifest.young.length >= 4) {
    await self.runCompaction();
  }
  fs.unlinkSync(path.join(self.directory, log_name));
  return true;
}

CrummyDB.prototype.addFileToManifest = async function (level, filename, min, max) {
  if (typeof this.manifest[level] == 'undefined') {
    this.manifest[level] = [];
  }
  this.manifest[level].push({ filename, min, max });
  await this.saveManifest();
  this.trimManifest();
}

CrummyDB.prototype.removeFileFromManifest = async function (level, filename) {
  var found = false;
  for (let i in this.manifest[level]) {
    if (this.manifest[level][i].filename == filename) {
      found = i;
      break;
    }
  }
  if (found !== false) {
    this.manifest[level].splice(found, 1);
    await this.saveManifest();
    this.trimManifest();
    fs.unlinkSync(filename);
  }
}

CrummyDB.prototype.trimManifest = async function () {
  var limit = 100;
  var trim = 50;
  var list = fs.readdirSync(path.join(this.directory, 'manifest'));
  if (list.length > limit) {
    for (let i = 0; i < list.length - trim; i++) {
      fs.unlinkSync(path.join(this.directory, 'manifest', list[i]));
    }
  }
}

CrummyDB.prototype.fileToMem = async function (file) {
  return new Promise(function (resolve, reject) {
    var mem_copy = {};
    var instream = fs.createReadStream(file);
    var outstream = new stream;
    var rl = readline.createInterface(instream, outstream);
    rl.on('line', function (line) {
      var tuple = line.split(SEP);
      mem_copy[tuple[0]] = tuple[1];
    });
    rl.on('close', function () {
      resolve(mem_copy);
    });
  });
}

CrummyDB.prototype.memToSST = async function (mem_table, directory) {
  return new Promise(async function (resolve, reject) {
    try {
      var limit = SSTMAX;
      var files = [];
      var time = Date.now();
      var filename = path.join(directory, time + '.sst');
      var fh = fs.createWriteStream(filename, { 'flags': 'a' });
      var total = 0;
      var keys = Object.keys(mem_table);
      keys.sort();
      var min_key = keys[0];
      var max_key = keys[0];

      for (let i in keys) {
        max_key = keys[i];
        if (total == 0) {
          min_key = keys[i];
        }
        var insert = keys[i] + SEP + mem_table[keys[i]] + '\n';
        total += Buffer.byteLength(insert, 'utf8');
        fh.write(insert);
        if (total > limit) {
          files.push({
            filename: filename,
            range: { min_key, max_key }
          });
          var o_time = time;
          if (time == o_time) {
            time += files.length;
          }

          total = 0;
          if (keys.length - 1 > i) {
            var filename = path.join(directory, time + '_' + files.length + '.sst');
            var fh = fs.createWriteStream(filename, { 'flags': 'a' });
          }
        }
      }

      if (total > 0) {
        files.push({
          filename: filename,
          range: { min_key, max_key }
        });
      }
      resolve(files);
    } catch (e) {
      reject(e);
    }
  });
}

CrummyDB.prototype.runCompaction = async function () {
  var self = this;
  var young_files = self.manifest['young'];
  var remove_young = [];
  var remove_l1 = [];
  var mem_temp = {};
  var merge_min = undefined;
  var merge_max = undefined;
  for (let i in young_files) {
    if (merge_min > young_files[i].min || merge_min == undefined) { merge_min = young_files[i].min; }
    if (merge_max < young_files[i].max || merge_max == undefined) { merge_max = young_files[i].max; }
    var temp = await self.fileToMem(young_files[i].filename);
    mem_table = Object.assign(mem_temp, temp);
    remove_young.push(young_files[i].filename);
  }
  if (typeof self.manifest['level1'] != 'undefined') {
    var l1_files = self.manifest['level1'];
    for (let i in l1_files) {
      if ((l1_files[i].min >= merge_min && l1_files[i].min <= merge_max)
        || (l1_files[i].max >= merge_min && l1_files[i].max <= merge_max)
        || (l1_files[i].min <= merge_min && l1_files[i].max >= merge_max)
      ) {
        var temp = await self.fileToMem(l1_files[i].filename);
        mem_table = Object.assign(temp, mem_table);
        if (remove_l1.indexOf(l1_files[i].filename)) {
          remove_l1.push(l1_files[i].filename);
        }
      }
    }
  }
  await ensureFullPath(path.join(self.directory, 'level1'));
  var directory = path.join(self.directory, 'level1');
  var files = await self.memToSST(mem_table, directory);
  for (let i in files) {
    var new_filename = files[i].filename;
    var range = files[i].range;
    await self.addFileToManifest('level1', new_filename, range.min_key, range.max_key);
  }
  for (let i in remove_young) {
    await self.removeFileFromManifest('young', remove_young[i]);
  }
  for (let i in remove_l1) {
    await self.removeFileFromManifest('level1', remove_l1[i]);
  }
  await self.compactLevel(1);
  return true;
}

CrummyDB.prototype.compactLevel = async function (level) {
  var self = this;
  var remove_my = [];
  var my_level = 'level' + level;
  var next_level = 'level' + (level + 1);
  var my_limit = Math.pow(10, level);
  if (typeof self.manifest[my_level] == 'undefined') {
    return;
  }
  var my_files = self.manifest[my_level];
  if (my_files.length <= my_limit) {
    return;
  }
  var remove_next = [];
  var mem_temp = {};
  var merge_min = undefined;
  var merge_max = undefined;
  var mem_temp = {};

  for (let i = 0; i < (my_files.length - my_limit) + 1; i++) {
    if (merge_min > my_files[i].min || merge_min == undefined) { merge_min = my_files[i].min; }
    if (merge_max < my_files[i].max || merge_max == undefined) { merge_max = my_files[i].max; }
    var temp = await self.fileToMem(my_files[i].filename);
    mem_temp = Object.assign(mem_temp, temp);
    remove_my.push(my_files[i].filename);
  }

  if (typeof self.manifest[next_level] != 'undefined') {
    var next_files = self.manifest[next_level];
    for (let i in next_files) {
      if ((next_files[i].min >= merge_min && next_files[i].min <= merge_max)
        || (next_files[i].max >= merge_min && next_files[i].max <= merge_max)
        || (next_files[i].min <= merge_min && next_files[i].max >= merge_max)
      ) {
        var temp = await self.fileToMem(next_files[i].filename);
        mem_temp = Object.assign(temp, mem_temp);
        if (remove_next.indexOf(next_files[i].filename)) {
          remove_next.push(next_files[i].filename);
        }
      }
    }
  }
  await ensureFullPath(path.join(self.directory, next_level));
  var directory = path.join(self.directory, next_level);
  var files = await self.memToSST(mem_temp, directory);
  for (let i in files) {
    var new_filename = files[i].filename;
    var range = files[i].range;
    await self.addFileToManifest(next_level, new_filename, range.min_key, range.max_key);
  }
  for (let i in remove_my) {
    await self.removeFileFromManifest(my_level, remove_my[i]);
  }
  for (let i in remove_next) {
    await self.removeFileFromManifest(next_level, remove_next[i]);
  }
  await this.compactLevel(level + 1);
  if (mem_temp['key1400'] != undefined) {
  }
  return;
}

CrummyDB.prototype.search = async function (key) {
  var self = this;
  return new Promise(async function (resolve, reject) {
    var value = false;
    if (typeof self.manifest['young'] != 'undefined') {
      for (let i = self.manifest['young'].length - 1; i >= 0; i--) {
        //check backwards because young files can have overlapping keys - so we want the most recent to take precedence
        if (self.manifest['young'][i].min <= key && self.manifest['young'][i].max >= key) {
          var value = await self.checkFile(self.manifest['young'][i].filename, key);
          break;
        }
      }
    }
    if (value) {
      resolve(value);
      return;
    } else {
      //<100 is dumb...
      for (let lvl = 1; lvl < 100; lvl++) {
        //assume if it's empty there are no upper.clea..?
        if (typeof self.manifest['level' + lvl] == 'undefined' || self.manifest['level' + lvl].length == 0) {
          resolve(value);
          return;
        }
        for (let i in self.manifest['level' + lvl]) {
          if (self.manifest['level' + lvl][i].min <= key && self.manifest['level' + lvl][i].max >= key) {
            var value = await self.checkFile(self.manifest['level' + lvl][i].filename, key);
            if (value !== false) {
              resolve(value);
              return;
            } else {
              break; //if we matched the range we can give up on this level, go to next
            }
          }
        }
      }
      //we should never make it here. this function is so ugly...
      resolve(value);
      return;
    }
  });
}

CrummyDB.prototype.checkFile = async function (filename, key) {
  //Can implement caching here if we want later
  var mem_copy = await this.fileToMem(filename);
  if (typeof mem_copy[key] != 'undefined') {
    return mem_copy[key];
  } else {
    return false;
  }
}

module.exports = CrummyDB;