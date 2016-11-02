"use strict"
/**
 * Copyright Tagtic
 * Created by leibosite
 * CreateTime: 16/10/26
 * desp:
 */

var fs = require('fs');
var LineByLineReader = require('line-by-line');
var ipbase = [16777216, 65536, 256, 1]; // for ip2long

var totalBlocks = 0;
var firstIndexPtr = 0;
var lastIndexPtr = 0;
var superBlock = new Buffer(8);

var indexBlockLength = 12;
var headerBlockLength = 8;
var totalHeaderLength = 4096;
var dataIndex = 8200;
var globalRegion = null;
var global_region_path = '/Users/leibosite/git/ip2region/data/global_region.csv';
var ip_merge_path = '/Users/leibosite/git/ip2region/data/ip.merge.txt';
var ip2region_path = '/Users/leibosite/git/ip2region/data/ip2region.db';
var my_ip2region_path = '/Users/leibosite/git/ip2region/data/my.ip2region.db'
var dbWriteFd = fs.openSync(my_ip2region_path, "w");


/**
 * get long value from buffer with specified offset
 * */
function getLong(buffer, offset) {
    var val = (
        (buffer[offset] & 0x000000FF) |
        ((buffer[offset + 1] << 8) & 0x0000FF00) |
        ((buffer[offset + 2] << 16) & 0x00FF0000) |
        ((buffer[offset + 3] << 24) & 0xFF000000)
    );

    // convert to unsigned int
    if (val < 0) {
        val = val >>> 0;
    }
    return val;
}

function getBuffer(data) {
    var buf = new Buffer(4);

    //将 long 型转化为 16进制
    buf[0] = data & 0x000000FF;
    buf[1] = (data & 0x0000FF00) >> 8;
    buf[2] = (data & 0x00FF0000) >> 16;
    buf[3] = (data & 0xFF000000) >> 24;
    return buf;

}
/**
 * convert ip to long (xxx.xxx.xxx.xxx to a integer)
 * */
function ip2long(ip) {
    var val = 0;
    ip.split('.').forEach(function (ele, i) {
        val += ipbase[i] * ele;
    });

    return val;
}

/**
 * get area Id from global_region.csv
 * @param arr
 * @param callback
 */
function getCityId(arr, callback) {

    var country = arr[0];
    var province = arr[2];
    var city = arr[3];

    if (!globalRegion) {

        var lr = new LineByLineReader(global_region_path);

        globalRegion = {};

        lr.on('error', function (err) {
            console.error(err);
        });

        lr.on('line', function (line) {
            var arr = line.split(',');
            globalRegion[arr[2]] = arr[0];
        });

        lr.on('end', function () {

            if (city != 0) {
                callback(globalRegion[city]);
            } else if (province != 0) {
                callback(globalRegion[province]);
            } else if (country != 0) {
                if ("未分配或者内网IP" === country) callback(0);
                callback(globalRegion[country]);
            }
        });
    } else {
        //console.log("globalRegion: "+globalRegion.length);

        if (city != 0) {
            callback(globalRegion[city]);
        } else if (province != 0) {
            callback(globalRegion[province]);
        } else if (country != 0) {
            if ("未分配或者内网IP" === country) callback(0);
            callback(globalRegion[country]);
        }
    }

}


/**
 * save data
 * @param txtPath
 * @param callback
 */
function saveDataAndRemoveDuplicates(txtPath, callback) {


    var lr = new LineByLineReader(txtPath);
    var dataPos = dataIndex;
    var areaObj = {};

    lr.on('line', function (line) {
        var arr = line.split('|');
        arr.shift();
        arr.shift();

        var key = arr.join('|');
        getCityId(arr, function (cityId) {

            var cityIdBuf = getBuffer(cityId);
            var data = new Buffer(key, 'utf8');
            data = Buffer.concat([cityIdBuf, data]);

            var dataIndex = getBuffer(dataPos);
            dataIndex[3] = data.length;

            //去重 存储 data,并记录
            if (!areaObj[key]) {

                areaObj[key] = dataIndex;
                fs.writeSync(dbWriteFd, data, 0, data.length, dataPos);
                dataPos += data.length;
            }
        });
    });

    lr.on('end', function () {

        console.log('------去重存储成功------', dataPos);

        callback([areaObj, dataPos]);
    });

}


/**
 * main function
 * save super index and save index
 */
function ipTxt2Db(){

    //调用getCityId 方法做初始化区域与cityId映射 globalRegion
    getCityId("中国|东北|吉林省|吉林市|教育网".split('|'), function (cityId) {

        saveDataAndRemoveDuplicates(ip_merge_path, function (result) {

            var areaResult = result[0];
            var indexPos = result[1];


            // superIndex firstIndex
            var firstIndexPtrData = getBuffer(indexPos);

            var headerCount = 0;
            var headerIndexBlock = null;
            var headerIndexPos = headerBlockLength;

            var lr = new LineByLineReader(ip_merge_path);
            lr.on('line', function (line) {
                var arr = line.split('|');
                var sipBuf = getBuffer(ip2long(arr.shift()));
                var eipBuf = getBuffer(ip2long(arr.shift()));
                var key = arr.join('|');

                //console.log('sip: ',sipBuf);
                //console.log('eip: ',eipBuf);
                //console.log('area: ',areaResult[key]);
                var indexBlock = Buffer.concat([sipBuf, eipBuf, areaResult[key]], indexBlockLength);

                //console.log(indexBlock);

                fs.writeSync(dbWriteFd, indexBlock, 0, indexBlockLength, indexPos);


                if (headerCount - 4096 >= 0 || headerCount == 0) {

                    headerIndexBlock = Buffer.concat([sipBuf, getBuffer(indexPos)], headerBlockLength);
                    fs.writeSync(dbWriteFd, headerIndexBlock, 0, headerBlockLength, headerIndexPos);
                    headerCount = headerCount - 4096;
                    headerIndexPos += headerBlockLength;
                }

                indexPos += indexBlockLength;
                headerCount += indexBlockLength;
            });

            lr.on('end', function () {
                console.log("---------存储结束---------", indexPos);
                var superIndexBlock = Buffer.concat([firstIndexPtrData, getBuffer(indexPos - indexBlockLength)], headerBlockLength);
                fs.writeSync(dbWriteFd, superIndexBlock, 0, headerBlockLength, 0);
                fs.closeSync(dbWriteFd);
            });
        });
    });
}

ipTxt2Db();
