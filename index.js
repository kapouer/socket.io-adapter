
/**
 * Module dependencies.
 */

var Emitter = require('events').EventEmitter;
var parser = require('socket.io-parser');

/**
 * Module exports.
 */

module.exports = Adapter;

/**
 * Memory adapter constructor.
 *
 * @param {Namespace} nsp
 * @api public
 */

function Adapter(nsp){
  this.nsp = nsp;
  this.rooms = {};
  this.sids = {};
  this.encoder = new parser.Encoder();
}

/**
 * Inherits from `EventEmitter`.
 */

Adapter.prototype.__proto__ = Emitter.prototype;

/**
 * Adds a socket from a room.
 *
 * @param {String} socket id
 * @param {String} room name
 * @param {Function} callback
 * @api public
 */

Adapter.prototype.add = function(id, room, fn){
  this.sids[id] = this.sids[id] || {};
  this.sids[id][room] = true;
  this.rooms[room] = this.rooms[room] || Room();
  this.rooms[room].add(id);
  if (fn) process.nextTick(fn.bind(null, null));
};

/**
 * Removes a socket from a room.
 *
 * @param {String} socket id
 * @param {String} room name
 * @param {Function} callback
 * @api public
 */

Adapter.prototype.del = function(id, room, fn){
  this.sids[id] = this.sids[id] || {};
  this.rooms[room] = this.rooms[room] || Room();
  delete this.sids[id][room];
  this.rooms[room].del(id);
  if (this.rooms[room].length === 0) {
    delete this.rooms[room];
  }

  if (fn) process.nextTick(fn.bind(null, null));
};

/**
 * Removes a socket from all rooms it's joined.
 *
 * @param {String} socket id
 * @api public
 */

Adapter.prototype.delAll = function(id, fn){
  var rooms = this.sids[id];
  if (rooms) {
    for (var room in rooms) {
      if (rooms.hasOwnProperty(room)) {
        this.rooms[room].del(id);
      }

      if (this.rooms[room].length === 0) {
        delete this.rooms[room];
      }
    }
  }
  delete this.sids[id];
};

/**
 * Broadcasts a packet.
 *
 * Options:
 *  - `flags` {Object} flags for this packet
 *  - `except` {Array} sids that should be excluded
 *  - `rooms` {Array} list of rooms to broadcast to
 *
 * @param {Object} packet object
 * @api public
 */

Adapter.prototype.broadcast = function(packet, opts){
  var rooms = opts.rooms || [];
  var except = opts.except || [];
  var flags = opts.flags || {};
  var ids = {};
  var self = this;
  var socket;

  packet.nsp = this.nsp.name;
  this.encoder.encode(packet, function(encodedPackets) {
    if (rooms.length) {
      for (var i = 0; i < rooms.length; i++) {
        var room = self.rooms[rooms[i]];
        if (!room) continue;
        var sockets = room.sockets;
        for (var id in sockets) {
          if (sockets.hasOwnProperty(id)) {
            if (ids[id] || ~except.indexOf(id)) continue;
            socket = self.nsp.connected[id];
            if (socket) {
              socket.packet(encodedPackets, true, flags.volatile);
              ids[id] = true;
            }
          }
        }
      }
    } else {
      for (var id in self.sids) {
        if (self.sids.hasOwnProperty(id)) {
          if (~except.indexOf(id)) continue;
          socket = self.nsp.connected[id];
          if (socket) socket.packet(encodedPackets, true, flags.volatile);
        }
      }
    }
  });
};

/**
 * Room constructor.
 *
 * @api private
 */

function Room() {
  if (!(this instanceof Room)) return new Room();
  this.sockets = {};
  this.length = 0;
}

Room.prototype.add = function(id) {
  if (this.sockets.hasOwnProperty(id)) return;
  else {
    this.sockets[id] = true;
    this.length++;
  }
};

Room.prototype.del = function(id) {
  if (!this.sockets.hasOwnProperty(id)) return;
  else {
    delete this.sockets[id];
    this.length--;
  }
};