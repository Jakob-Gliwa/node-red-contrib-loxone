module.exports = function (RED) {
    "use strict";
    const node_lox_ws_api = require("node-lox-ws-api");
    const http = require('http');
    const encMethods = {
        0: 'Token-Enc',
        1: 'AES-256-CBC',
        2: 'Hash'
    };

    RED.httpAdmin.get('/loxone-miniserver/struct', function (req, res) {
        if (!req.query.id) {
            return res.json("");
        }

        let configNode = RED.nodes.getNode(req.query.id);
        let result = {
            state: 'error',
            msg: 'miniserver not connected',
            structure: {}
        };

        if (!configNode) {
            result.msg = 'Could not get config node!'
            res.json(result);
            return;
        }

        if (configNode.active !== true) {
            result.msg = 'connection disabled'
        }

        if (configNode && configNode.active && configNode.connected) {
            result = {
                state: 'ok',
                msg: 'got miniserver structure',
                structure: configNode.structureData
            };
        }

        res.json(result);
    });

    RED.httpAdmin.get('/loxone-miniserver/struct-changed', function (req, res) {
        let result = {
            state: 'error',
            msg: 'miniserver not connected',
            structure: {}
        };

        let username = req.query.username;
        let password = req.query.password;
        let configNode = RED.nodes.getNode(req.query.id);
        if (configNode) {
            if (!username) {
                username = configNode.credentials.username;
            }
            if (!password) {
                password = configNode.credentials.password;
            }

            if (configNode.active !== true) {
                result.msg = 'connection disabled'
            }
        }

        http.get({
                host: req.query.host,
                port: req.query.port,
                path: '/data/LoxAPP3.json',
                auth: username + ':' + password,
            }, function (http_res) {
                if (http_res.statusCode !== 200) {
                    http_res.resume();
                    res.json(result);
                    return;
                }

                let data = '';
                http_res.on('data', function (chunk) {
                    data += chunk;
                });
                http_res.on('end', function () {
                    result = {
                        state: 'ok',
                        msg: 'got miniserver structure',
                        structure: prepareStructure(JSON.parse(data)),
                    };
                    res.json(result);
                });
            }
        ).on('error', function (e) {
            res.json(result);
        });
    });

    function LoxoneMiniserver(config) {
        function _updateEvent(uuid, evt) {
            node.handleEvent(uuid, evt);
        }

        function _limitString(text, limit) {
            if (text.length <= limit) {
                return text;
            }
            return text.substr(0, limit) + '...(' + text.length + ')';
        }

        RED.nodes.createNode(this, config);

        let node = this;
        node.connected = false;
        node.authenticated = false;
        node.connection = null;
        node.structureData = null;
        node.active = true; //config.active;
        node._onlineNodes = [];
        node._keepaliveNodes = [];  // Performance: direct array instead of eachNode()
        node._inputNodes = [];
        node._outputNodes = [];
        node._streamInNodes = [];
        node._streamAllNodes = [];
        node._webserviceNodes = [];
        node._webserviceNodeQueue = []; //only webservice nodes which are waiting for a return will be here
        node._otherEvents = [];

        // TODO
        //do nothing if miniserver connection is not active
        /*
        if (config.active !== true) {
            node.log('connection to ' + config.host + ':' + config.port + ' disabled');

            node.setConnectionStatusMsg("grey", "connection disabled", "dot");
            return;
        }*/

        let text_logger_limit = 100;

        node.encMethod = 'Token-Enc';
        if (encMethods.hasOwnProperty(config.enctype)) {
            node.encMethod = encMethods[config.enctype];
        }

        if (!node.credentials.username || !node.credentials.password) {
            node.error("missing credentials for " + config.host + ':' + config.port);
            return;
        }

        let client = new node_lox_ws_api(
            config.host + ':' + config.port,
            node.credentials.username,
            node.credentials.password,
            true,
            node.encMethod,
            config.keepalive
        );

        client.connect();

        client.on('connect', function () {
            node.log('Miniserver connected (' + config.host + ':' + config.port + ') using ' + node.encMethod);
        });

        client.on('authorized', function () {
            node.authenticated = true;
            node.connection = client;
        });

        client.on('get_structure_file', function (data) {
            node.log("got structure file " + data.lastModified);
            node.structureData = prepareStructure(data);
            node.connected = true;
            node.setConnectionStatusMsg("green", "connected", "dot");
            sendOnlineNodeMsg(true, config.id);
        });

        client.on('connect_failed', function (error, reason) {
            node.error('Miniserver connect failed: ' + reason, JSON.stringify(error));
            node.setConnectionStatusMsg("red", "connection failed", "ring");
        });

        client.on('connection_error', function (error, reason) {
            node.error('Miniserver connection error: ' + reason, JSON.stringify(error));
            node.setConnectionStatusMsg("red", "connection error", "ring");
        });

        client.on('close', function (info, reason) {
            node.log("connection closed: " + reason, info);
            node.connected = false;
            node.authenticated = false;
            node.connection = null;


            //node.log('active on client close' + node.active);

            /*if (node.active !== true) {
                client.abort();
                node.setConnectionStatusMsg("grey", "connection disabled", "ring");
            } else {
                node.setConnectionStatusMsg("yellow", "connection closed", "ring");
            }*/

            node.setConnectionStatusMsg("yellow", "connection closed", "ring");
            sendOnlineNodeMsg(false, config.id);
        });

        client.on('send', function (message) {
            //node.log("sent message: " + message);
        });

        client.on('message_text', function (message) {
            switch (message.type) {
                case 'json':
                    data.json = _limitString(JSON.stringify(message.json), text_logger_limit);
                    node.log("received text message: " + data.json);

                    break;
                case 'control':
                    for (let i in node._webserviceNodeQueue) {
                        const wsNode = node._webserviceNodeQueue[i];
                        const handler = wsNode.handler;
                        const nodeMsg = wsNode.msg;
                        const uri = wsNode.uri;
                        if (uri === 'j' + message.control) {
                            let msg = Object.assign(nodeMsg, {
                                'payload': message.value,
                                'topic': message.control,
                                'code': parseInt(message.code)
                            });

                            //and parse all values to msg.data
                            if (message.hasOwnProperty('data') && message.data.hasOwnProperty('LL')) {
                                let additionalData = JSON.parse(JSON.stringify(message.data.LL));
                                delete additionalData.Code;
                                delete additionalData.control;

                                if (Object.keys(additionalData).length) {
                                    msg.data = additionalData;
                                }
                            }

                            //add miniserver info
                            if (node.structureData) {
                                msg.msInfo = node.structureData.msInfo;
                                msg.lastModified = node.structureData.lastModified;
                            } else {
                                msg.msInfo = null;
                                msg.lastModified = null;

                                node.log("structure data not ready yet, webservice request processed but without msInfo and lastModified");
                            }

                            //send the data out of the requesting node
                            handler.send(msg);

                            //unregister node from queue
                            node.removeWebserviceNodeFromQueue(handler, uri);
                            break;
                        }
                    }

                    break;
                default:
                    data.text = _limitString(message.data, text_logger_limit);
                    node.log("received text message: " + data.text);
            }
        });

        client.on('keepalive', function (time) {
            // Performance: Direct iteration instead of eachNode()
            for (let i = 0; i < node._keepaliveNodes.length; i++) {
                node._keepaliveNodes[i].send({
                    topic: 'keepalive',
                    payload: time,
                    msInfo: node.structureData.msInfo,
                    lastModified: node.structureData.lastModified
                });
            }
        });

        client.on('message_header', function (header) {
            //node.log('received message header (' + header.next_state() + '):');
            //console.log(header);
        });

        client.on('message_event_table_values', function (messages) {
            //node.log('received value messages:' + messages.length);
        });

        client.on('message_event_table_text', function (messages) {
            //node.log('received text messages:' + messages.length);
        });

        client.on('update_event_value', _updateEvent);
        client.on('update_event_text', _updateEvent);
        client.on('update_event_daytimer', _updateEvent);
        client.on('update_event_weather', _updateEvent);

        this.on('close', function (done) {
            //on (full-)deploys close event shutdown the client
            if (node.connected) {
                client.once('close', function () {
                    done();
                });
                client.abort();
            } else {
                done();
            }

        });
    }

    RED.nodes.registerType("loxone-miniserver", LoxoneMiniserver, {
        credentials: {
            username: {type: "text"},
            password: {type: "password"}
        }
    });

    LoxoneMiniserver.prototype.registerOnlineNode = function (handler) {
        this._onlineNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerInputNode = function (handler) {
        this._inputNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerOutputNode = function (handler) {
        this._outputNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerWebserviceNode = function (handler) {
        this._webserviceNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerStreamInNode = function (handler) {
        this._streamInNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerStreamAllNode = function (handler) {
        this._streamAllNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerKeepaliveNode = function (handler) {
        if (this._keepaliveNodes.indexOf(handler) === -1) {
            this._keepaliveNodes.push(handler);
        }
    };

    LoxoneMiniserver.prototype.deregisterKeepaliveNode = function (handler) {
        const idx = this._keepaliveNodes.indexOf(handler);
        if (idx !== -1) {
            this._keepaliveNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.addWebserviceNodeToQueue = function (handler, msg, uri) {
        this._webserviceNodeQueue.push({
            'handler': handler,
            'msg': msg,
            'uri': uri
        });
    };

    LoxoneMiniserver.prototype.deregisterOnlineNode = function (handler) {
        const idx = this._onlineNodes.indexOf(handler);
        if (idx !== -1) {
            this._onlineNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.deregisterInputNode = function (handler) {
        const idx = this._inputNodes.indexOf(handler);
        if (idx !== -1) {
            this._inputNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.deregisterOutputNode = function (handler) {
        const idx = this._outputNodes.indexOf(handler);
        if (idx !== -1) {
            this._outputNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.deregisterStreamInNode = function (handler) {
        const idx = this._streamInNodes.indexOf(handler);
        if (idx !== -1) {
            this._streamInNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.deregisterStreamAllNode = function (handler) {
        const idx = this._streamAllNodes.indexOf(handler);
        if (idx !== -1) {
            this._streamAllNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.deregisterWebserviceNode = function (handler) {
        const idx = this._webserviceNodes.indexOf(handler);
        if (idx !== -1) {
            this._webserviceNodes.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.removeWebserviceNodeFromQueue = function (handler, uri) {
        const idx = this._webserviceNodeQueue.findIndex(function (node) {
            return node.handler === handler && node.uri === uri;
        });
        if (idx !== -1) {
            this._webserviceNodeQueue.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.setConnectionStatusMsg = function (color, text, shape) {
        shape = shape || 'dot';
        let newState = function (item) {
            item.status({
                fill: color,
                shape: shape,
                text: text
            });
        };

        this._inputNodes.forEach(newState);
        this._outputNodes.forEach(newState);
        this._streamInNodes.forEach(newState);
        this._streamAllNodes.forEach(newState);
        this._webserviceNodes.forEach(newState);
        this._onlineNodes.forEach(newState);
    };

    LoxoneMiniserver.prototype.findControlByState = function (uuid) {
        // O(1) lookup via Map instead of O(n) linear search
        const entry = this.structureData._stateIndex.get(uuid);
        return entry ? entry.control : null;
    };

    LoxoneMiniserver.prototype.buildMsgObject = function (event, uuid, controlStructure) {
        //get state name
        let stateName;
        for (stateName in controlStructure.states) {
            if (
                controlStructure.states.hasOwnProperty(stateName) &&
                (controlStructure.states[stateName] === uuid || controlStructure.states[stateName].indexOf(uuid) > -1)
            ) {

                if (typeof controlStructure.states[stateName] !== 'string' && controlStructure.states[stateName].indexOf(uuid) > -1) {
                    stateName += ':' + controlStructure.states[stateName].indexOf(uuid);
                }

                break;
            }
        }

        //evaluate payload
        let payload;

        try {
            payload = JSON.parse(event);
        } catch (err) {
            payload = event;
        }

        //check if control has a room
        let room = null;
        if (controlStructure.room && this.structureData.rooms[controlStructure.room].name) {
            room = this.structureData.rooms[controlStructure.room].name;
        }

        //check if control has a category
        let category = null;
        if (controlStructure.cat && this.structureData.cats[controlStructure.cat].name) {
            category = this.structureData.cats[controlStructure.cat].name;
        }

        return {
            payload: payload,
            topic: controlStructure.name || null,
            state: stateName,
            room: room,
            category: category,
            details: controlStructure.details || null,
            type: controlStructure.type || null,
            isFavorite: controlStructure.isFavorite || null,
            isSecured: controlStructure.isSecured || null,
            uuid: uuid || null,
            uuidAction: controlStructure.uuidAction || null,
            msInfo: this.structureData.msInfo,
            lastModified: this.structureData.lastModified
        };
    };

    LoxoneMiniserver.prototype.handleEvent = function (uuid, event) {
        let i, curNode, curRoom, curCategory;
        let controlStructure = this.findControlByState(uuid);

        //do we have a control for this uuid? could also be weather or global
        if (controlStructure) {

            //publish event to control-in nodes
            for (i = 0; i < this._inputNodes.length; i++) {
                if (this._inputNodes[i].state === uuid) {
                    this._inputNodes[i].send(this.buildMsgObject(event, uuid, controlStructure));
                }
            }

            //publish event to stream-all node
            for (i = 0; i < this._streamAllNodes.length; i++) {
                curNode = this._streamAllNodes[i];
                try {
                    curNode.send(this.buildMsgObject(event, uuid, controlStructure));
                } catch (error) {
                    //TODO: error handling
                }
            }

            //publish event to stream-in node
            for (i = 0; i < this._streamInNodes.length; i++) {
                curNode = this._streamInNodes[i];
                curRoom = (curNode.hasOwnProperty('room')) ? curNode.room : null;
                curCategory = (curNode.hasOwnProperty('category')) ? curNode.category : null;

                //neither category or room present/filled, continue
                if (!curRoom && !curCategory) {
                    continue;
                }

                if (
                    (!curRoom || curRoom === controlStructure.room) &&
                    (!curCategory || curCategory === controlStructure.cat)
                ) {
                    curNode.send(this.buildMsgObject(event, uuid, controlStructure));
                }
            }
        }

    };

    function prepareStructure(data) {
        let structure = {
            rooms: data.rooms || {},
            cats: data.cats || {},
            controls: {},
            msInfo: data.msInfo || {},
            lastModified: data.lastModified,
            _stateIndex: new Map()  // Performance: O(1) lookup for findControlByState
        };

        // Pass 1: Build controls structure (parent first, then subcontrols)
        for (let uuid in data.controls) {
            if (!data.controls.hasOwnProperty(uuid)) {
                continue;
            }

            structure.controls[uuid] = data.controls[uuid];
            if (data.controls[uuid].hasOwnProperty('subControls')) {
                for (let sub_uuid in data.controls[uuid].subControls) {
                    if (!data.controls[uuid].subControls.hasOwnProperty(sub_uuid)) {
                        continue;
                    }

                    structure.controls[sub_uuid] = data.controls[uuid].subControls[sub_uuid];
                    structure.controls[sub_uuid].parent = uuid;
                    structure.controls[sub_uuid].room = data.controls[uuid].room;
                    structure.controls[sub_uuid].cat = data.controls[uuid].cat;
                }
            }
        }

        // Pass 2: Build state index for O(1) lookups
        // Parent controls are processed first, so they get priority if UUID appears in multiple places
        for (let controlUuid in structure.controls) {
            if (!structure.controls.hasOwnProperty(controlUuid)) {
                continue;
            }

            const control = structure.controls[controlUuid];
            if (!control.states) {
                continue;
            }

            for (let stateName in control.states) {
                if (!control.states.hasOwnProperty(stateName)) {
                    continue;
                }

                const stateValue = control.states[stateName];

                // Handle both string UUIDs and array of UUIDs
                if (typeof stateValue === 'string') {
                    // Single UUID string
                    if (!structure._stateIndex.has(stateValue)) {
                        structure._stateIndex.set(stateValue, {
                            control: control,
                            stateName: stateName
                        });
                    }
                } else if (Array.isArray(stateValue)) {
                    // Array of UUIDs (e.g., for multi-value states)
                    for (let i = 0; i < stateValue.length; i++) {
                        const uuid = stateValue[i];
                        if (typeof uuid === 'string' && !structure._stateIndex.has(uuid)) {
                            structure._stateIndex.set(uuid, {
                                control: control,
                                stateName: stateName,
                                arrayIndex: i  // Store array index for later use
                            });
                        }
                    }
                }
            }
        }

        return structure;
    }

    function sendOnlineNodeMsg(online, configId) {
        online = online || false;

        // Performance: Direct iteration instead of eachNode()
        const miniserver = RED.nodes.getNode(configId);
        if (miniserver && miniserver._onlineNodes) {
            for (let i = 0; i < miniserver._onlineNodes.length; i++) {
                miniserver._onlineNodes[i].send({
                    payload: online
                });
            }
        }
    }

    function LoxoneControlInNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.state = config.state;
        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerInputNode(node);
            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterInputNode(node);
                }

                done();
            });
        }
    }

    RED.nodes.registerType("loxone-control-in", LoxoneControlInNode);

    function LoxoneControlOutNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {

            node.miniserver.registerOutputNode(node);

            this.on('input', function (msg) {
                if (node.miniserver.connected && node.miniserver.connection) {
                    node.miniserver.connection.send_control_command(node.control, msg.payload);
                } else {
                    node.log(node.id + ' was triggered but miniserver is not connected.');
                }
            });

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterOutputNode(node);
                }
                done();
            });
        }

    }

    RED.nodes.registerType("loxone-control-out", LoxoneControlOutNode);

    function LoxoneWebServiceNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerWebserviceNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterWebserviceNode(node);
                }
                done();
            });

            this.on('input', function (msg) {
                let wantedURI = msg.uri || config.uri;
                if (!wantedURI.length) {
                    node.status({
                        fill: 'red',
                        shape: 'ring',
                        text: 'empty uri'
                    });
                    return null;
                }

                if (!wantedURI.match(/^jdev/)) {
                    node.status({
                        fill: 'red',
                        shape: 'ring',
                        text: 'invalid uri'
                    });
                    return null;
                }

                node.uri = wantedURI + ((config.appendpayload === true) ? msg.payload : '');

                //add node to the queue for waiting messages and send the URI
                if (node.miniserver.connected && node.miniserver.connection) {
                    node.miniserver.addWebserviceNodeToQueue(node, msg, node.uri);
                    node.miniserver.connection.send_command(node.uri);
                } else {
                    node.status({
                        fill: 'red',
                        shape: 'dot',
                        text: 'not connected'
                    });
                    node.log(node.id + " tried to call " + wantedURI + " but miniserver is not connected.");
                    return null;
                }
            });
        }
    }

    RED.nodes.registerType('loxone-webservice', LoxoneWebServiceNode);

    function LoxoneOnlineNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.state = config.state;
        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerOnlineNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterOnlineNode(node);
                }
                done();
            });
        }
    }

    RED.nodes.registerType('loxone-online', LoxoneOnlineNode);

    function LoxoneKeepaliveNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerKeepaliveNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterKeepaliveNode(node);
                }
                done();
            });
        }
    }

    RED.nodes.registerType('loxone-keepalive', LoxoneKeepaliveNode);
    RED.nodes.registerType('loxone-stream-in', LoxoneStreamInNode);

    function LoxoneStreamInNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.category = config.category;
        node.room = config.room;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {

            node.miniserver.registerStreamInNode(node);

            this.on('close', function (done) {

                if (node.miniserver) {
                    node.miniserver.deregisterStreamInNode(node);
                }

                done();
            });
        }
    }

    RED.nodes.registerType('loxone-stream-all', LoxoneStreamAllNode);

    function LoxoneStreamAllNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.category = config.category;
        node.room = config.room;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerStreamAllNode(node);

            this.on('close', function (done) {

                if (node.miniserver) {
                    node.miniserver.deregisterStreamAllNode(node);
                }

                done();
            });
        }
    }
};
