/* global Node */


var Service = (function () {
    var NS = "http://rawrepo.dbc.dk/maintain/";
    /**
     * Funtions that given a string returns an array of values
     * Used for validation, and value processing during soaprequest building
     * 
     * @type type
     */
    var valueFilter = {
        any: function (value) {
            return [value];
        },
        agencyid: function (value) {
            if (value.match(/^[0-9]{6}$/) === null)
                throw Error("Agencyid are required");
            return [value];
        },
        agencyidnotdbc: function (value) {
            if (value.match(/^191919|870970$/) !== null)
                throw Error("Agencyid " + value + " is not allowed");
            if (value.match(/^[0-9]{6}$/) === null)
                throw Error("Agencyid are required");
            return [value];
        },
        lines: function (value) {
            var l = value.match(/[^\r\n]+/g);
            if (l === null || l.length === 0 && l[0] === '')
                throw Error("Lines are required");
            return l;
        },
        word: function (value) {
            if (value.match(/\s/) || value === '')
                throw Error("value: '" + value + "' is not a word");
            return [value];
        },
        words: function (value) {
            if (value === '' || !value.match(/\S/))
                throw Error("value: '" + value + "' is not words");
            return value.match(/(\S)+/);
        },
        token: function (value) {
            if (value.match(/[^a-z0-9_-]/i) || value === '')
                throw Error("value: '" + value + "' is not a token");
            return [value];
        },
        tokens: function (value) {
            if (value.match(/[^\sa-z0-9_-]/i) || value.match(/^\s*$/))
                throw Error("value: '" + value + "' is not tokens");
            return value.match(/(\S)+/);
        },
        number: function (value) {
            if (value.match(/[^0-9]/) || value === '')
                throw Error("value: '" + value + "' is not a number");
            return [value];
        },
        numbers: function (value) {
            if (value.match(/[^0-9\s]/) || value === '')
                throw Error("value: '" + value + "' is not numbers");
            return value.split(/(?:\r?\n)+/);
        },
        timestamp: function (value) {
            var m = moment(value, ["YYYY-MM-DD H:mm:ss.SSS", "YYYY-MM-DD H:mm:ss", "YYYY-MM-DD"], true);
            if (!m.isValid())
                throw Error("value: '" + value + "' is not a timestamp");
            return [m.toDate().getTime()];
        },
        timestampms: function (value) {
            var m = moment(value, ["YYYY-MM-DD H:mm:ss.SSS"], true);
            if (!m.isValid())
                throw Error("value: '" + value + "' is not a timestamp");
            return [m.toDate().getTime()];
        }
    };
    /**
     * Fake a "Log" method, allow for not logging in IE+
     * 
     */
    var Log = function () {
    };
    try {
        Log = console.log.bind(console);
    } catch (e) {

    }

    /**
     * 
     * @type SoapResponse
     */
    var SoapResponse = (function () {

        /**
         * Construct a SoapResponse from the xml text
         * 
         * new SoapResponse('<?xml?><S:Envelope ....');
         * 
         * @param {type} xml
         * @returns {Service.SoapResponse}
         */
        var that = function (xml) {
            var parser = new DOMParser();
            this.xml = parser.parseFromString(xml, 'application/xml');
            var base = this.xml.querySelector(":root > Body > *");
            if (base === null) {
                errorShow("Not really a SOAP response ?");
                this.prefix = "";
            } else {
                this.prefix = ":root > Body > " + base.localName;
            }
        };
        /**
         * build a ":root > Body:nth-child(1) > .." path to a node
         * 
         * @param {HTMLElement} node
         * @returns {String}
         */
        var buildPrefix = function (node) {
            if (node.parentNode.nodeType !== Node.ELEMENT_NODE)
                return  ":root";
            else {
                var i = 0;
                var prev = node;
                while (prev !== null) {
                    i++;
                    prev = prev.previousSibling;
                }
                return buildPrefix(node.parentNode) + " > " + node.localName + ":nth-child(" + i + ")";
            }
        };
        /**
         * build a full selector path based in base of default node
         * 
         * @param {Service.SoapResponse} that
         * @param {String} path "//" -> space, "/" -> ">"
         * @param {HTMLElement} base
         * @returns {String}
         */
        var buildFullPath = function (that, path, base) {
            var prefix = that.prefix;
            if (base !== undefined) {
                prefix = buildPrefix(base);
            }
            path = path.replace(/^\/{0,2}/, function (a) {
                return {
                    '': ' > ',
                    '/': ' > ',
                    '//': ' '
                }[a];
            });
            path = path.replace(/\/{1,2}/g, function (a) {
                return {
                    '/': ' > ',
                    '//': ' '
                }[a];
            });
            return prefix + path;
        };
        /**
         * Select one node based on "/" & "//" path expression with default base or optional base
         * 
         * @param {type} path xpath delimited path elements
         * @param {type} base (optional) root node
         * @returns {Node}
         */
        that.prototype.node = function (path, base) {
            return  this.xml.querySelector(buildFullPath(this, path, base));
        };
        /**
         * @see node
         * 
         * @param {type} path
         * @param {type} base
         * @returns {Boolean} existance of node
         */
        that.prototype.bool = function (path, base) {
            return this.node(path, base) !== null;
        };
        /**
         * @see node
         * 
         * @param {type} path
         * @param {type} base
         * @returns {String} content of node
         */
        that.prototype.text = function (path, base) {
            var node = this.node(path, base);
            return node && node.textContent;
        };
        /**
         * @see buildFullPath
         * 
         * @param {type} that
         * @param {type} path
         * @param {type} base 
         * @param {type} func (optional) node processor
         * @returns {Function} iterator function, each call returns next node
         */
        var iter = function (that, path, base, func) {
            return  (function (list) {
                var pos = 0;
                var that = function () {
                    if (pos >= list.length)
                        return null;
                    var value = list.item(pos++);
                    if (typeof (func) === 'function')
                        value = func(value);
                    return value;
                };
                that.asArray = function () {
                    var array = [];
                    for (var value = this(); value !== null; value = this()) {
                        array.push(value);
                    }
                    return array;
                }.bind(that);
                return that;
            })(that.xml.querySelectorAll(buildFullPath(that, path, base)));
        };
        /**
         * @see node
         * 
         * @param {type} path
         * @param {type} base
         * @returns {Function} iterator, each call returns next node, ends with null
         */
        that.prototype.iter = function (path, base) {
            return iter(this, path, base);
        };
        /**
         * @see node
         * 
         * @param {type} path
         * @param {type} base
         * @returns {Function} iterator, each call returns next nodes text, ends with null
         */
        that.prototype.iterText = function (path, base) {
            return iter(this, path, base, function (node) {
                return node.textContent;
            });
        };
        return that;
    })();
    /**
     * 
     * @type SoapRequest
     */
    var SoapRequest = (function () {
        /**
         * 
         * @param {String} method
         * @param {String} ns
         * @returns {Service.SoapRequest}
         */
        var that = function (method, ns) {
            if (ns === undefined)
                ns = NS;
            var p = new DOMParser();
            this.doc = p.parseFromString('<S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/"/>', "text/xml");
            var env = this.doc.firstChild;
            var body = this.doc.createElement("S:Body");
            env.appendChild(body);
            this.base = this.doc.createElementNS(ns, method);
            body.appendChild(this.base);
        };
        /**
         * 
         * @returns {String}
         */
        that.prototype.toString = function () {
            return new XMLSerializer().serializeToString(this.doc, 2);
        };
        /**
         * 
         * @param {String} path
         * @param {Array|String} elements
         * @returns {undefined}
         */
        that.prototype.add = function (path, elements) {
            if (!elements instanceof Array || typeof (elements) === "string")
                elements = [elements];
            var parts = path.split('/');
            var root = this.base;
            var target = parts.pop();
            while (parts.length > 0) {
                var part = parts.shift();
                if (part === '')
                    break;
                if (root.lastChild === null || root.lastChild.nodeName !== part) {
                    if (part[0] === ':') {
                        root.appendChild(this.doc.createElement(part.substr(1)));
                    } else {
                        root.appendChild(this.doc.createElementNS(this.base.namespaceURI, part));
                    }
                }
                root = root.lastChild;
            }
            parts.push(target);
            for (var j = 0; j < elements.length; j++) {
                var target = root;
                for (var i = 0; i < parts.length; i++) {
                    var child = this.doc.createElementNS(this.base.namespaceURI, parts[i]);
                    target.appendChild(child);
                    target = child;
                }
                target.appendChild(this.doc.createTextNode(elements[j]));
            }
        };
        /**
         * Show standard response (diag & status)
         * 
         * @param {Service.SoapResponse} soapResponse
         * @returns {undefined}
         */
        var processStandardResponse = function (soapResponse) {
            if (soapResponse === null) {

            }
            if (soapResponse.bool("error")) {
                var text = "Error: " + soapResponse.text("error/type") + "\n" + soapResponse.text("error/message");
                errorShow(text, true);
            } else if (soapResponse.bool("result")) {
                var diagIter = soapResponse.iter("result/diags/diag");
                for (var diag = diagIter(); diag !== null; diag = diagIter()) {
                    var title = soapResponse.text("title", diag);
                    var message = soapResponse.text("message", diag);
                    errorShow("Diagnostics: " + title + "\n" + message);
                }
                var status = soapResponse.text('result/status');
                showStatus(soapResponse.text('result/message'), status === 'SUCCESS');
            } else {
                errorShow("Unknown response", true);
            }
        };
        /**
         * 
         * @param {function(Service.SoapResponse|null)} func
         * @returns {undefined}
         */
        that.prototype.send = function (func) {
            if (func === undefined)
                func = processStandardResponse;
            var req = new XMLHttpRequest();
            enableSpinner();
            req.onreadystatechange = function () {
                if (req.readyState === 4) {
                    disableSpinner();
                    if (req.status === 200) {
                        var xp = new SoapResponse(req.responseText);
                        func(xp);
                    } else {
                        var xml = req.responseXML;
                        if (xml !== null) {
                            var col = xml.getElementsByTagName('faultstring');
                            if (col.length === 1) {
                                errorShow("Request Error:\n" + col.item(0).textContent, true);
                                return;
                            }
                        }
                        errorShow(req.responseText, true);
                        func(null);
                    }
                }
            };
            req.open('POST', 'Maintain', true);
            req.setRequestHeader('Content-Type', 'text/xml');
            req.send(this.toString());
        };
        /**
         * 
         * @param {HTMLElement} element
         * @returns {undefined | {}}
         */
        var valuesOf = function (element) {
            if (element.name === '')
                return undefined;
            var m = element.name.match(/^(.*)#(\??)(.*)/);
            if (m === null) {
                return {
                    name: element.name,
                    values: [element.value],
                    value: [element.value]
                };
            }
            if (m[2] === '?' && element.value === '') {
                return undefined;
            }
            try {
                return  {
                    name: m[1],
                    values: (valueFilter[m[3]] || valueFilter.any)(element.value),
                    value: [element.value]
                };
            } catch (e) {
                throw Error(e.message + "\nin field: " + m[1]);
            }
        };
        /**
         * 
         * @param {type} soapRequest
         * @returns {Function}
         */
        var sendRequestTraverse = function (soapRequest) {

            var methods = {
                INPUT: function (a) {
                    if (a.values.length > 1)
                        throw Error(a.name + "produces multiple values..");
                    soapRequest.add(a.name, a.values.length === 1 ? a.values : a.value);
                },
                SELECT: function (a) {
                    if (a.values.length > 1)
                        throw Error(a.name + "produces multiple values..");
                    soapRequest.add(a.name, a.values.length === 1 ? a.values : a.value);
                },
                TEXTAREA: function (a) {
                    soapRequest.add(a.name, a.values);
                }
            };
            return function (element) {
                if (element.nodeName in methods) {
                    var a = valuesOf(element);
                    if (a === undefined)
                        return;
                    methods[element.nodeName](a);
                }
            };
        };
        /**
         * 
         * @param {String} name
         * @param {undefined|function(Service.SoapRequest|null)} func
         * @returns {undefined}
         */
        that.sendRequest = function (name, func) {
            try {
                var soapRequest = new SoapRequest(name + "Request");
                var element = document.getElementById(name);
                traverse(element, sendRequestTraverse(soapRequest));
                soapRequest.send(func);
            } catch (e) {
                errorShow(e, true);
            }
        };
        /**
         * 
         * @param {type} soapRequest
         * @returns {Function} for use in traverse(HTMLElement, Function), puts data in soapRequest
         */
        var pageContentTraverse = function (soapRequest) {

            var methods = {
                INPUT: function (a) {
                    soapRequest.add('values//entry/key', a.name);
                    soapRequest.add('values/entry/value', a.value);
                },
                SELECT: function (a) {
                    soapRequest.add('values//entry/key', a.name);
                    soapRequest.add('values/entry/value', a.value);
                },
                TEXTAREA: function (a) {
                    soapRequest.add('values//entry/key', a.name);
                    soapRequest.add('values/entry/value', a.values);
                }
            };
            return function (element) {
                try {
                    if (element.nodeName in methods) {
                        var a = valuesOf(element);
                        if (a === undefined)
                            return;
                        methods[element.nodeName](a);
                    }
                } catch (e) {
                }
            };
        };
        /**
         * 
         * @param {HTMLElement} element
         * @returns {Function} for Service.SoapRequest.send(Function)
         */
        var pageContentFill = function (element) {
            var values = {
            };
            var methods = {
                INPUT: function (node, values) {
                    if (values.length === 1) {
                        node.value = values[0];
                    } else if (values.length > 1) {
                        var sel = document.createElement('select');
                        sel.name = node.name;
                        node.parentNode.replaceChild(sel, node);
                        if (node.name.indexOf('#?') >= 0) {
                            var opt = document.createElement('option');
                            opt.value = '';
                            opt.appendChild(document.createTextNode("[Select One]"));
                            sel.appendChild(opt);
                        }
                        methods.SELECT(sel, values);
                    }
                },
                SELECT: function (node, values) {
                    var value = node.value;
                    var child = node.firstChild;
                    while (child !== null) {
                        var current = child;
                        child = child.nextSibling;
                        if (current.textContent === current.value)
                            node.removeChild(current);
                    }
                    for (var i = 0; i < values.length; i++) {
                        var opt = document.createElement('option');
                        opt.appendChild(document.createTextNode(values[i]));
                        opt.selected = values[i] === value;
                        node.appendChild(opt);
                    }

                },
                TEXTAREA: function (node, values) {
                    node.value = values.join("\n");
                }
            };
            var fillElement = function (node) {
                if (node.nodeName in methods) {
                    var name = (node.name + '#').split('#', 2)[0];
                    if (name in values) {
                        methods[node.nodeName](node, values[name]);
                    }
                }
            };
            /**
             * 
             * @param {Service.SoapResponse} soapResponse
             */
            return function (soapResponse) {
                if (soapResponse !== undefined) {
                    var entries = soapResponse.iter('result/values/entry');
                    for (var entry = entries(); entry !== null; entry = entries()) {
                        var name = soapResponse.text('key', entry);
                        values[name] = soapResponse.iterText('value', entry).asArray();
                    }
                }
                traverse(element, fillElement);
                validateAllFields(element);
            };
        };
        that.pageContent = function (element, leaving) {
            if (typeof (element) === 'string')
                element = document.getElementById(element);
            var soapRequest = new SoapRequest("pageContentRequest");
            soapRequest.add("method", element.id);
            if(leaving !== undefined)
                soapRequest.add("leaving", leaving);
            traverse(element, pageContentTraverse(soapRequest));
            soapRequest.send(pageContentFill(element));
        };
        return that;
    })();
    /**
     * Add a class to a domElement (.classList.add)
     * 
     * @param {type} element
     * @param {type} clazz class to add
     * @returns {undefined}
     */
    var addClass = function (element, clazz) {
        var a = (element.className || "").match(/\S+/) || [];
        for (var i = 0; i < a.length; i++) {
            if (a[i] === clazz)
                return;
        }
        a.push(clazz);
        element.className = a.join(" ");
    };
    /**
     * Remove a class from a domElement (.classList.remove)
     * 
     * @param {type} element
     * @param {type} clazz class to remove
     * @returns {undefined}
     */
    var removeClass = function (element, clazz) {
        var a = (element.className || "").match(/\S+/) || [];
        a = a.filter(function (c) {
            return c !== clazz;
        });
        element.className = a.join(" ");
    };
    /**
     * set/clear class "invalid" on element, if name="#..."
     * and valueFilter[...] doesn't throw an error
     * 
     * @param {type} element domElement that has .name & .value
     * @returns {undefined}
     */
    var validateField = function (element) {
        try {
            var a = ("" + element.name).match(/#(\??)(\w+)$/);
            if (a === null)
                return;
            if (a[1] === '?') {
                if (element.value === "") {
                    removeClass(element, "invalid");
                    return;
                }
            }
            if (!(a[2] in valueFilter))
                return;
            valueFilter[a[2]](element.value);
            removeClass(element, "invalid");
        } catch (e) {
            addClass(element, "invalid");
        }
    };
    /**
     * Traverses a domTree calling func(node) on each node in turn
     * 
     * if func returns a value (other than undefined) treaversal stops
     * and the value is returned
     * 
     * @param {type} element domelement
     * @param {type} func function
     * @returns {unresolved} value if func() or undefined
     */
    var traverse = function (element, func) {
        var ret = undefined;
        if (element.nodeType === Node.ELEMENT_NODE) {
            ret = func(element);
            for (var child = element.firstChild; child !== null && ret === undefined; child = child.nextSibling) {
                ret = traverse(child, func);
            }
        }
        return ret;
    };
    /**
     * Traverses a domTree and validates all INPUT/SELECT/TEXTAREA
     * 
     * Also sets inputchange methods, to autiomatically validate
     * 
     * @param {type} element
     * @returns {undefined}
     */
    var validateAllFields = (function (element) {
        var nodeNames = {
            INPUT: ['oninput'],
            SELECT: ['onchange'],
            TEXTAREA: ['oninput']
        };
        var validateElement = function (element) {
            if (element.nodeName in nodeNames) {
                if (element.name.indexOf('#') >= 0) {
                    var triggers = nodeNames[element.nodeName];
                    var trigger = (function (element) {
                        return function () {
                            validateField(element);
                        };
                    })(element);
                    for (var i = 0; i < triggers.length; i++) {
                        element[triggers[i]] = trigger;
                    }
                    trigger();
                }
            }
        };
        return function (element) {
            traverse(element, validateElement);
        };
    })();
    /**
     * Loads find from a <input type="file"> and puts it into the previous <textarea>
     * 
     * used as onchange="Service.loadFileHandler(event, this);"
     * 
     * @param {type} event
     * @returns {undefined}
     */
    var loadFileHandler = function (event) {
        var that = event.target;
        var fileReader = new FileReader();
        fileReader.onload = function (e) {
            that.value = "";
            if (e.target.error !== null) {
                errorShow("cannot load file: " + e.target.error, true);
            } else if (e.target.result) {
                while (that !== null && that.nodeName !== 'TEXTAREA') {
                    that = that.previousSibling;
                }
                if (that !== null) {
                    that.value = e.target.result;
                    validateField(that);
                } else {
                    errorShow("Error no textarea found", true);
                }
            }
        };
        fileReader.readAsText(event.target.files[0]);
    };
    /**
     * converts previous sibling SELECT to an <input>, with an automatic
     * fieldvalidator
     * 
     * used as onclick="Service.toText(this);"
     * @param {type} that
     * @returns {undefined}
     */

    var toText = function (that) {
        var sel = that.previousSibling;
        if (sel.nodeName !== 'SELECT') {
            return;
        }
        var text = document.createElement('input');
        text.name = sel.name;
        text.value = sel.value;
        if (text.name.indexOf('#') >= 0) {
            text.oninput = function () {
                validateField(text);
            };
            validateField(text);
        }
        that.parentNode.replaceChild(text, sel);
        that.parentNode.removeChild(that);
        text.focus();
    };
    /**
     * Builds an error message element and puts it in the "errors" area
     * 
     * @param {type} message text
     * @param {type} fatal (turns readish)
     * @returns {Element} domNode containing message
     */
    var errorShow = function (message, fatal) {
        var target = document.getElementById('errors');
        var root = document.createElement('div');
        root.className = fatal ? 'error-message-fatal' : 'error-message';
        target.appendChild(root);
        var top = document.createElement('div');
        top.className = 'error-header';
        root.appendChild(top);
        var close = document.createElement('a');
        close.href = '#';
        close.className = 'error-close';
        close.onclick = function () {
            target.removeChild(root);
        };
        top.appendChild(close);
        close.appendChild(document.createTextNode('x'));
        var body = document.createElement('div');
        body.className = 'error-body';
        root.appendChild(body);
        if (message !== undefined) {
            body.appendChild(document.createTextNode(message));
        }
        return body;
    };
    /**
     * Remove all errors
     * 
     * @returns {undefined}
     */
    var errorsClear = function () {
        var target = document.getElementById('errors');
        var child = target.firstChild;
        while (child !== null) {
            var next = child.nextSibling;
            if (child.nodeType === Node.ELEMENT_NODE &&
                    child.nodeName === 'DIV') {
                target.removeChild(child);
            }
            child = next;
        }
    };
    var currentSpinner = null;
    /**
     * Start spinner in 1/10th of a sec
     * 
     * @returns {undefined}
     */
    var enableSpinner = function () {
        if (currentSpinner !== null)
            clearTimeout(currentSpinner);
        currentSpinner = setTimeout(function () {
            addClass(document.getElementById('spinner'), 'selected');
        }, 100);
    };
    /**
     * Stops spinner (and upcoming)
     * 
     * @returns {undefined}
     */
    var disableSpinner = function () {
        if (currentSpinner !== null) {
            clearTimeout(currentSpinner);
            currentSpinner = null;
        }
        removeClass(document.getElementById('spinner'), 'selected');
    };
    var pages = {
        tabs: [],
        panels: [],
        content: [],
        welcome: null
    };

    /**
     * Coverts an id to a pagenumber
     * 
     * @param {Number|String} page
     * @returns {Number}
     */
    var findPage = function (page) {
        if (typeof (page) === 'number')
            return page;
        for (var i = 0; i < pages.panels.length; i++) {
            if (pages.panels[i].id === page)
                return i;
        }
        return 0;
    };

    /**
     * Reload saved html content for a page
     * 
     * @param {Number|String} page
     * @returns {undefined}
     */
    var resetPage = function (page) {
        var no = findPage(page);
        pages.panels[no].innerHTML = pages.content[no];
        SoapRequest.pageContent(pages.panels[no]);
    };

    /**
     * Highlight tab of a page
     * 
     * @param {Number} no
     * @returns {undefined}
     */
    var highlightTab = function (no) {
        if (pages.welcome !== null)
            removeClass(pages.welcome, 'selected');
        for (var i = 0; i < pages.tabs.length; i++) {
            removeClass(pages.tabs[i], 'selected');
            removeClass(pages.panels[i], 'selected');
        }
        addClass(pages.tabs[no], 'selected');
        addClass(pages.panels[no], 'selected');
    };

    /**
     * Switch a page
     * 
     * @param {Number|String} page
     * @returns {undefined}
     */
    var switchToPage = function (page) {
        var no = findPage(page);
        var panel = pages.panels[no];
        if (panel.hasChildNodes()) {
            highlightTab(no);
        } else {
            highlightTab(no);
            resetPage(no);
        }
        traverse(pages.panels[no], function (node) {
            if (node.nodeName.match(/^(INPUT|SELECT|TEXTAREA)$/)) {
                node.focus();
                return true;
            }
        });
    };

    /**
     * Process the id="content" tag, saving pages building tabs
     * 
     * @returns {undefined}
     */
    var constructContent = function () {
        var content = document.getElementById("content");
        var titles = document.getElementById("titles");
        var elem = content.firstChild;
        if (elem.nodeType === Node.TEXT_NODE
                && elem.nodeValue.trim() === "") {
            elem = elem.nextSibling;
        }
        var selectOne = true;
        if (elem.nodeType === Node.ELEMENT_NODE) {
            pages.welcome = elem;
            addClass(elem, 'selected');
            selectOne = false;
            elem = elem.nextSibling;
        }

        while (elem !== null) {
            if (elem.nodeType !== Node.TEXT_NODE) {
                errorShow("Content of 'content' is text, element, text, element, ...");
            }
            var title = elem;
            elem = elem.nextSibling;
            content.removeChild(title);
            title = title.nodeValue.trim();
            if (elem === null) {
                break;
            }
            if (elem.nodeType !== Node.ELEMENT_NODE) {
                errorShow("Content of 'content' is text, element, text, element, ...");
            }
            var page = elem;
            elem = elem.nextSibling;
            var i = pages.tabs.length;
            var a = document.createElement("a");
            pages.tabs.push(a);
            a.href = '#';
            a.onclick = (function (i) {
                return function () {
                    switchToPage(i);
                };
            })(i);
            a.ondblclick = (function (i) {
                return function () {
                    resetPage(i);
                };
            })(i);
            a.appendChild(document.createTextNode(title));
            titles.appendChild(a);
            pages.panels.push(page);
            pages.content.push(page.innerHTML);
            page.onkeypress = keypress;
            page.innerHTML = '';
        }
        if (selectOne)
            switchToPage(0);
        disableSpinner();
    };

    /**
     * key event handler
     * 
     * @param {KeyEvent} e
     * @returns {Boolean}
     */
    var keypress = function (e) {
        if (e.keyCode === 13) {
            var name = e.target.nodeName;
            if (name.match(/^A|TEXTAREA$/) === null) {
                var c = this.getElementsByClassName('cr');
                if (c.length === 1) {
                    var cr = c.item(0);
                    if (typeof (cr.onclick) === 'function') {
                        cr.onclick();
                        return false;
                    }
                }
            }
        }
        return true;
    };
    var keydown = function (e) {
        if (e.ctrlKey && e.keyCode >= 48 && e.keyCode <= 57) { // '0' - '9'
            var no = (e.keyCode - 48 + 9) % 10;
            if (no >= pages.panels.length)
                return false;
            switchToPage(no);
            return false;
        }
        if (e.keyCode === 27) {
            closeStatus();
            errorsClear();
            return false;
        }
        return true;
    };
    /**
     * Show status message
     * 
     * @param {String} text processed with marked(...)
     * @param {type} autoclose (Modal)
     * @returns {service_L4.showStatus.body}
     */
    var showStatus = function (text, autoclose) {
        var status = document.getElementById('status');
        var body = document.getElementById('status-body');
        while (body.hasChildNodes()) {
            body.removeChild(body.firstChild);
        }
        if (text !== undefined)
            body.innerHTML = marked(text);
        body.scrollTop = 0;
        addClass(status, 'selected');
        if (autoclose) {
            setTimeout(closeStatus, 2500);
        } else {
            addClass(status, 'error');
        }
        return body;
    };

    /**
     * 
     * @returns {undefined}
     */
    var closeStatus = function () {
        var status = document.getElementById('status');
        removeClass(status, 'selected');
    };
    
    return {
        SoapRequest: SoapRequest,
        SoapResponse: SoapResponse,
        closeStatus: closeStatus,
        constructContent: constructContent,
        disableSpinner: disableSpinner,
        enableSpinner: enableSpinner,
        errorShow: errorShow,
        errorsClear: errorsClear,
        keypress: keypress,
        keydown: keydown,
        loadFileHandler: loadFileHandler,
        resetPage: resetPage,
        showStatus: showStatus,
        switchToPage: switchToPage,
        toText: toText,
        traverse: traverse,
        validateField: validateField
    };
    
})();