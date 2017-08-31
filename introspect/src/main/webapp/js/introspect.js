/*
 * dbc-rawrepo-introspect
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-introspect.
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-introspect.  If not, see <http://www.gnu.org/licenses/>.
 */
$('document').ready(function () {

    /**
     * Fallback for console.log
     *
     * @returns {undefined}
     */
    var LOG = function () {
    };
    try {
        console.log("init log");
        LOG = console.log.bind(console);
    } catch (e) {
    }

    /**
     * Show error for 3 sek on page
     *
     * @param {type} text
     * @returns {undefined}
     */
    var ERROR = function (text) {
        var d = $('<div class="ui-button ui-widget ui-corner-all ui-state-default"/>');
        d.text(text);
        $('#errors').append(d);
        setTimeout(function () {
            d.remove();
        }, 3000);
    };

    /**
     *
     * @returns {Function}
     */
    var ajaxBuilder = function () {
        var cache = {};
        var callbacks = {};

        return function (url, callback) {
            if (url in cache) {
                callback(cache[url]);
            } else if (url in callbacks) {
                callbacks[url].push(callback);
            } else {
                callbacks[url] = [callback];
                $.ajax({
                    type: "GET",
                    dataType: "json",
                    url: url,
                    success: function (data, textStatus, jqXHR) {
                        cache[url] = data;
                        callbacks[url].forEach(function (func) {
                            func(data);
                        });
                        delete callbacks[url];
                    },
                    error: function (jqXHR, textStatus, errorThrown) {
                        ERROR(errorThrown);
                    }
                });
            }
        };
    };
    /**
     * Caching ajax call with callback queue
     *
     * @param {string} url
     * @param {function} callback
     * @type Function
     */
    var ajax = ajaxBuilder();

    var clearCache = function () {
        ajax = ajaxBuilder();
    };

    /**
     * Caching ajax call with callback queue
     *
     * @param {string} url
     * @param {function} callback
     * @type Function
     */


    /**
     * DisplayPane Class
     *
     * @type Function|DisplayPane
     */
    var DisplayPane = (function () {

        /**
         * Constructor
         *
         * @returns {take2_L65.DisplayPane}
         */
        var DisplayPane = function () {
            this.db = null;
            this.idInput = $('#record-key');
            this.versionSelect = $('#version');
            this.otherVersionSelect = $('#other-version');
            this.content = $('#content');
            this.history = $('#history');

            var versionSelected = this.versionSelected.bind(this);
            this.versionSelect.selectmenu({select: versionSelected});
            this.otherVersionSelect.selectmenu({select: versionSelected});

            this.lineformatter = $('#lineformatter');
            this.lineformatter.button();
            this.lineformatter.on('click', this.lineformat.bind(this));
            $(window).on('resize', this.resize.bind(this));

            return this;
        };

        DisplayPane.prototype.lineformat = function () {
            var key = this.idInput.val();
            var arg = key.split(",", 3);
            if (arg.length !== 3)
                return;
            var agencyid = arg[1];
            var bibliographicrecordid = arg[2];

            ajax("resources/lineformatter/" + this.db + "/" + agencyid + "/" + bibliographicrecordid,
                this.lineformatterFunction.bind(this));
        };

        DisplayPane.prototype.lineformatterFunction = function (data) {
            this.content.contents().remove();
            var that = this;
            var dataHtml = $('<span class="' + "data" + '"/>');
            if (data.size < 1) {
                dataHtml.text("Could not find data for recordid ");
            } else {
                dataHtml.text(data[0])
            }
            that.content.append(dataHtml);
            this.resize();
        };

        /**
         * Internal when a version or other version, has been selected
         *
         * @param {type} event
         * @param {type} ui
         * @returns {undefined}
         */
        DisplayPane.prototype.versionSelected = function (event, ui) {
            var key = this.idInput.val();
            var arg = key.split(",", 3);
            if (arg.length !== 3)
                return;
            var agencyid = arg[1];
            var bibliographicrecordid = arg[2];
            var thisVersion = this.versionSelect.val();
            var otherVersion = this.otherVersionSelect.val();
            if (thisVersion === '')
                return;
            if (thisVersion === '-1') {
                this.otherVersionSelect.selectmenu('disable');
                ajax("resources/record-merged/" + this.db + "/" + agencyid + "/" + bibliographicrecordid,
                    this.displayRecord.bind(this));
            } else {
                this.otherVersionSelect.selectmenu('enable');
                if (otherVersion === '') {
                    ajax("resources/record-historic/" + this.db + "/" + agencyid + "/" + bibliographicrecordid + "/" + thisVersion,
                        this.displayRecord.bind(this));
                } else {
                    ajax("resources/record-diff/" + this.db + "/" + agencyid + "/" + bibliographicrecordid + "/" + thisVersion + "/" + otherVersion,
                        this.displayRecord.bind(this));
                }
            }
        };


        /**
         * Set an Id (Key) for the pane
         *
         * @param {type} val
         * @returns {undefined}
         */
        DisplayPane.prototype.setId = function (val) {
            if (this.idInput.val() !== val) {
                this.idInput.val(val);
                var arg = val.split(",", 3);
                if (arg.length !== 3)
                    return;
                this.db = arg[0];
                var agencyid = arg[1];
                var bibliographicrecordid = arg[2];
                this.clear();
                ajax("resources/record-history/" + this.db + "/" + agencyid + "/" + bibliographicrecordid,
                    this.historyRead.bind(this));
                ajax("resources/relations/" + this.db + "/" + agencyid + "/" + bibliographicrecordid,
                    this.relationsRead.bind(this));
            }
        };

        /**
         * Internal: Clean content of version(s)
         *
         * @returns {undefined}
         */
        DisplayPane.prototype.clear = function () {
            [this.versionSelect, this.otherVersionSelect].forEach(function (tag, i) {
                tag.children().each(function (i) {
                    if (i > 0)
                        $(this).remove();
                });
            });
            this.content.contents().remove();
            this.history.contents().remove();
        };

        /**
         * Internal: Callback for history ajax call
         *
         * @param {type} data
         * @returns {undefined}
         */
        DisplayPane.prototype.historyRead = function (data) {
            [this.versionSelect, this.otherVersionSelect].forEach(function (tag, i) {
                tag.children().each(function (i) {
                    if (i > 0)
                        $(this).remove();
                });
                data.forEach(function (e, i) {
                    var option = $('<option/>');
                    var deleted = e['deleted'] ? 'DELETED' : ''; // Append "DELETED" if the record is deleted
                    option.attr({value: i}).text(e['modified'] + " (" + e['mimetype'] + ") " + deleted);
                    $('#version').append(option);
                    tag.append(option);
                });
                tag.selectmenu('refresh');
                tag.selectmenu('enable');
            });
            this.displayHistory(data);
        };

        /**
         * Internal: Callback for relations ajax call
         *
         * @param {type} data
         * @returns {undefined}
         */
        DisplayPane.prototype.relationsRead = function (data) {
            if (data['siblings-out'].length > 0) {
                $('#version option:first-child').first().after($('<option value="-1">Merge</option>'));
                $('#version').selectmenu('refresh');
            }
        };

        /**
         * Internal: Callback for document fetched ajax call
         *
         * @param {type} data
         * @returns {undefined}
         */
        DisplayPane.prototype.displayRecord = function (data) {
            var that = this;
            this.content.contents().remove();
            data.forEach(function (e) {
                var f = $('<span class="' + e.type + '"/>');
                f.text(e.content);
                that.content.append(f);
            });
            this.resize();
        };

        DisplayPane.prototype.displayHistory = function (data) {
            var that = this;
            this.history.contents().remove();
            data.forEach(function (e) {
                var f;
                if (e['deleted']) {
                    f = $('<span title="Status: Deleted | Mimetype: ' + e['mimetype'] + '" style="color:red"/>');
                } else {
                    f = $('<span title="Status: Active | Mimetype: ' + e['mimetype'] + '"/>');
                }
                var modified = e['modified'].length === 22 ? e['modified'] + '0' : e['modified'];

                f.text(modified);
                f.append('<br>');
                that.history.append(f);
            });
            this.resize();
        };


        /**
         * Resize content area to fit screen
         *
         * @returns {undefined}
         */
        DisplayPane.prototype.resize = function () {
            var containerContent = document.getElementById('content-container');
            var containerHistory = document.getElementById('history-container');
            var parentContainer = document.getElementById('recordTabs-content');
            var bodyHeight = parseInt(window.getComputedStyle(document.body).height);
            var bodyWidth = parseInt(window.getComputedStyle(document.body).width);
            var windowHeight = window.innerHeight;
            var windowsWidth = window.innerWidth;
            if (bodyHeight > windowHeight || windowHeight - bodyHeight > 20) {
                var height = parseInt(window.getComputedStyle(containerContent).height);
                var width = parseInt(window.getComputedStyle(parentContainer).width);
                height = Math.max(height - (bodyHeight - windowHeight), 300);
                height = height - 25;
                containerContent.style.height = height + "px";
                containerHistory.style.height = height + "px";
                width = Math.max(width - (bodyWidth - windowsWidth), 300);
                containerHistory.style.width = "220px";
                containerContent.style.width = width - 275 + "px";
            }
        };

        return DisplayPane;
    })();

    /**
     * RelationPane Class
     *
     * @type RelationPane Constructor
     */
    var RelationPane = (function () {

        /**
         * Constants
         *
         * @type type
         */
        var LINKDISTANCE = {
            parent: 40,
            sibling: 16
        };
        var COLOR = {
            nodeSelected: 'white',
            nodeUnexpanded: 'blue',
            nodeDeselected: 'green',
            linkParent: 'steelblue',
            linkSibling: 'orchid',
            linkParentOutbound: 'lightsteelblue',
            linkSiblingOutbound: 'pink',
            unknown: 'white'
        };


        /**
         * Constructor
         *
         * @returns {take2_L237.RelationPane}
         */
        var RelationPane = function () {
            this.db = null;
            this.collection = {};
            this.nodes = [];
            this.links = [];
            this.current = null;
            this.selected = null;
            this.onHover = null;
            this.onSelect = null;
            this.onClick = null;
            this.selectWidget = $('#record-select');
            this.hoverWidget = $('#record-hover');
            this.centerWidget = $('#record-center');
            this.width = 500;
            this.height = 300;
            this.domParent = d3.select('#svg-container');
            this.svg = this.domParent
                .append("svg")
                .attr("width", this.width)
                .attr("height", this.height);
            this.canvas = this.svg.append('g');
            this.force = d3.layout.force()
                .size([this.width, this.height])
                .distance(function (d) { // proportional to relation type & combined number of relations
                    var weight = 1;
                    if (d.source.content !== null)
                        weight = weight + d.source.content.weight;
                    if (d.target.content !== null)
                        weight = weight + d.target.content.weight;
                    var factor = Math.pow(1.025, weight) + 1;
                    return Math.min(this.width * 0.3, this.height * 0.3, (LINKDISTANCE[d.type]) * factor);
                }.bind(this))
                .friction(.3)
                .gravity(0.00)
                .charge(-500)
                .on("tick", this.tick.bind(this));

            var resize = this.resize.bind(this);
            $(window).on('resize', resize);
            this.centerWidget.on('click', this.center.bind(this));
            resize();
            return this;
        };

        /**
         * Clears widgets
         *
         * @returns {undefined}
         */
        RelationPane.prototype.clear = function () {
            this.hoverWidget.val('');
            this.selectWidget.val('');
        };

        /**
         * Set id (key) and render svg accordingly
         *
         * @param {type} key
         * @returns {undefined}
         */
        RelationPane.prototype.setId = function (key) {
            this.collection = {};
            this.nodes = [];
            this.links = [];
            this.current = null;
            this.selected = null;
            this.canvas.selectAll('.node').remove();
            this.canvas.selectAll('.link').remove();
            this.clear();
            this.refresh();
            var arg = key.split(",", 3);
            if (arg.length !== 3)
                return;
            this.db = arg[0];
            this.fetchRelations(key);
        };

        /**
         * Internal: Fetch relations for id (key)
         *
         * @param {type} key
         * @returns {undefined}
         */
        RelationPane.prototype.fetchRelations = function (key) {
            if (key in this.collection && this.collection[key].content !== null)
                return;
            var arg = key.split(",", 3);
            if (arg.length !== 3)
                return;
            var agencyid = arg[1];
            var bibliographicrecordid = arg[2];
            ajax("resources/relations/" + this.db + "/" + agencyid + "/" + bibliographicrecordid,
                this.relationsFetched.bind(this));
        };

        /**
         * Internal: Callback for relations ajax call
         *
         * @param {type} record
         * @returns {undefined}
         */
        RelationPane.prototype.relationsFetched = function (record) {
            var key = this.db + "," + record['me'].agencyid + "," + record['me'].bibliographicrecordid;
            record.key = key;
            record.weight = (
                Object.keys(record['parents']).length +
                Object.keys(record['children']).length +
                Object.keys(record['siblings-in']).length +
                Object.keys(record['siblings-out']).length
            );
            if (!(key in this.collection)) {
                var d = {key: key, content: record, relations: {}};
                this.collection[key] = d;
                this.nodes.push(d);
            } else if (this.collection[key].content === null) {
                this.collection[key].content = record;
            }
            this.ensureNodes(record['parents']);
            this.ensureNodes(record['children']);
            this.ensureNodes(record['siblings-out']);
            this.ensureNodes(record['siblings-in']);
            record['parents'].forEach(function (e) {
                this.ensureLink(key, e.key, 'parent');
            }, this);
            record['children'].forEach(function (e) {
                this.ensureLink(e.key, key, 'parent');
            }, this);
            record['siblings-out'].forEach(function (e) {
                this.ensureLink(key, e.key, 'sibling');
            }, this);
            record['siblings-in'].forEach(function (e) {
                this.ensureLink(e.key, key, 'sibling');
            }, this);
            this.refresh();
        };

        /**
         * Internal: Ensure all relations exists as node objects
         *
         * @param {type} rels
         * @returns {undefined}
         */
        RelationPane.prototype.ensureNodes = function (rels) {
            rels.forEach(function (e) {
                var key = this.db + "," + e.agencyid + "," + e.bibliographicrecordid;
                e.key = key;
                if (!(key in this.collection)) {
                    var d = {key: key, content: null, relations: {}};
                    this.collection[key] = d;
                    this.nodes.push(d);
                }
            }, this);
        };

        /**
         * Internal: Ensure a link exists as a link object
         *
         * @param {type} source
         * @param {type} target
         * @param {type} type
         * @returns {undefined}
         */
        RelationPane.prototype.ensureLink = function (source, target, type) {
            if (!(target in this.collection[source].relations)) {
                this.collection[source].relations[target] = true;
                this.links.push({source: this.collection[source], target: this.collection[target], type: type});
            }
        };

        /**
         * Internal: refresh noded/links, used when nodes has been added
         *
         * @returns {undefined}
         */
        RelationPane.prototype.refresh = function () {
            if (this.current === null && this.nodes.length > 0) {
                this.current = this.nodes[0].key;
            }

            this.force
                .nodes(this.nodes)
                .links(this.links)
                .start();

            this.canvas.selectAll('.link')
                .data(this.force.links())
                .enter()
                .append('path')
                .attr('class', 'link')
                .style('fill', function (d) {
                    return d.type === 'parent' ? (d.source.key === this.selected ? COLOR.linkParentOutbound : COLOR.linkParent) :
                        d.type === 'sibling' ? (d.source.key === this.selected ? COLOR.linkSiblingOutbound : COLOR.linkSibling) :
                            COLOR.unknown;
                }.bind(this))
                .style('stroke', function (d) {
                    return d.type === 'parent' ? (d.source.key === this.selected ? COLOR.linkParentOutbound : COLOR.linkParent) :
                        d.type === 'sibling' ? (d.source.key === this.selected ? COLOR.linkSiblingOutbound : COLOR.linkSibling) :
                            COLOR.unknown;
                }.bind(this))
                .style('stroke-width', function (d) {
                    return d.source.key === this.selected ? 2.0 : 1.25;
                }.bind(this));

            this.canvas.selectAll('.node')
                .data(this.force.nodes())
                .enter()
                .append('circle')
                .attr('class', 'node')
                .each(function (d, i) {
                    d.node = d3.select(this);
                })
                .attr('r', 5)
                .style('stroke', 'black')
                .style('fill', function (d) {
                    return d.content === null ? COLOR.nodeUnexpanded :
                        COLOR.nodeDeselected;
                }.bind(this))
                .on('mouseover', function (d) {
                    if (this.onSelect !== null && this.selected === d.key)
                        return;
                    this.hoverWidget.val(d.key);
                    if (this.onHover !== null)
                        this.onHover(d.key);
                }.bind(this))
                .on('mouseout', function (d) {
                    this.hoverWidget.val('');
                    if (this.onHover !== null)
                        this.onHover(null);
                }.bind(this))
                .on('mousedown', function (d) {
                    if (this.selected !== null && this.selected !== d.key) {
                        this.collection[this.selected].fixed = false;
                    }
                }.bind(this))
                .on('click', function (d) {
                    if (d3.event.defaultPrevented)
                        return; // ignore drag
                    if (d.content === null) {
                        this.fetchRelations(d.key);
                        d.fixed = true;
                    }
                    this.setHighlight(d.key);
                }.bind(this))
                .on('dblclick', function (d) {
                    if (this.onClick !== null)
                        this.onClick(d.key);
                }.bind(this))
                .call(this.force.drag);

            if (this.selected === null &&
                this.nodes.length > 0) {
                this.setHighlight(this.nodes[0].key);
            }

        };

        RelationPane.prototype.center = function () {

            if (this.selected !== null) {
                var obj = this.collection[this.selected];
                var x = this.width / 2;
                var y = this.height / 2;
                this.force.stop();
                this.canvas
                    .selectAll('.node')
                    .data(this.nodes)
                    .each(function (d) {
                        d.fixed = d === obj;
                        if (d.fixed) {
                            d.px = x;
                            d.py = y;
                        }
                    });
                this.force.start();
            }
        };


        /**
         * Resize canvas according to parent size
         *
         * @returns {undefined}
         */

        RelationPane.prototype.resize = function () {
            var container = document.getElementById('svg-container');
            var style = window.getComputedStyle(container);
            var width = Math.max(parseInt(style.width), 500);
            var height = Math.max(parseInt(style.height), 300);
            var bodyHeight = parseInt(window.getComputedStyle(document.body).height);
            var windowHeight = window.innerHeight;
            if (bodyHeight > windowHeight || windowHeight - bodyHeight > 20) {
                height = Math.max(height - (bodyHeight - windowHeight), 300);
            }
            height = height - 30;
            container.style.height = height + "px";
            container.style.width = "100%";
            this.height = height;
            this.width = width;
            this.svg.attr("width", this.width)
                .attr("height", this.height);
            this.force.size([this.width, this.height]);
            this.force.start();
        };


        /**
         * Internal: highlight node that is selected
         *
         * @param {type} selected
         * @returns {undefined}
         */
        RelationPane.prototype.setHighlight = function (selected) {
            this.selectWidget.val(selected);
            if (this.selected !== selected) {
                if (this.onSelect !== null) {
                    this.onSelect(selected);
                    this.hoverWidget.val('');
                    if (this.onHover !== null) {
                        this.onHover(null);
                    }
                }
                this.selected = selected;

                var t = d3.transition()
                    .duration(1000);
                t.selectAll('.node')
                    .style('fill', function (d, i) {
                        return d.key === this.current ? COLOR.nodeSelected :
                            d.content === null && d.key !== this.selected ? COLOR.nodeUnexpanded :
                                COLOR.nodeDeselected;
                    }.bind(this))
                    .attr('r', function (d) {
                        return d.key === this.selected ? 10 : 5;
                    }.bind(this));
                t.selectAll('.link')
                    .style('fill', function (d) {
                        return d.type === 'parent' ? (d.source.key === this.selected ? COLOR.linkParentOutbound : COLOR.linkParent) :
                            d.type === 'sibling' ? (d.source.key === this.selected ? COLOR.linkSiblingOutbound : COLOR.linkSibling) :
                                COLOR.unknown;
                    }.bind(this))
                    .style('stroke', function (d) {
                        return d.type === 'parent' ? (d.source.key === this.selected ? COLOR.linkParentOutbound : COLOR.linkParent) :
                            d.type === 'sibling' ? (d.source.key === this.selected ? COLOR.linkSiblingOutbound : COLOR.linkSibling) :
                                COLOR.unknown;
                    }.bind(this))
                    .style('stroke-width', function (d) {
                        return d.source.key === this.selected ? 2.0 : 1.25;
                    }.bind(this))
                    .call(this.force.start);
            }
        };

        /**
         * Set callback
         *
         * @param {type} onSelect
         * @returns {undefined}
         */
        RelationPane.prototype.setOnSelect = function (onSelect) {
            this.onSelect = onSelect;
        };

        /**
         * Set callback
         *
         * @param {type} onHover
         * @returns {undefined}
         */
        RelationPane.prototype.setOnHover = function (onHover) {
            this.onHover = onHover;
        };

        /**
         * Set callback
         *
         * @param {type} onClick
         * @returns {undefined}
         */
        RelationPane.prototype.setOnClick = function (onClick) {
            this.onClick = onClick;
        };

        /**
         * Start animation
         *
         * @returns {undefined}
         */
        RelationPane.prototype.start = function () {
            this.force.start();
        };

        /**
         * Stop animation
         *
         * @returns {undefined}
         */
        RelationPane.prototype.stop = function () {
            this.force.stop();
        };

        /**
         * Internal: Describe svg links
         *
         * @param {type} d
         * @returns {String}
         */
        RelationPane.prototype.d = function (d) {
            var dxa = d.target.x - d.source.x,
                dya = d.target.y - d.source.y,
                dra = Math.sqrt(dxa * dxa + dya * dya),
                sr = d.source.node.attr('r'),
                tr = d.target.node.attr('r'),
                sx = d.source.x + (dxa * sr / dra),
                sy = d.source.y + (dya * sr / dra),
                tx = d.target.x - (dxa * tr / dra),
                ty = d.target.y - (dya * tr / dra),
                dx = tx - sx,
                dy = ty - sy,
                theta = Math.atan2(dy, dx),
                d90 = Math.PI / 2,
                dtxs = tx - 3 * Math.cos(theta),
                dtys = ty - 3 * Math.sin(theta),
                l = 8,
                w = 2.5
            ;
            return "M" + sx + "," + sy +
                "L" + tx + "," + ty +
                "M" + dtxs + "," + dtys +
                "l" + (w * Math.cos(d90 - theta) - l * Math.cos(theta)) + "," + (-w * Math.sin(d90 - theta) - l * Math.sin(theta)) +
                "L" + (dtxs - w * Math.cos(d90 - theta) - l * Math.cos(theta)) + "," + (dtys + w * Math.sin(d90 - theta) - l * Math.sin(theta)) +
                "z";
        };

        /**
         * Internal: layout.force tick function
         * @returns {undefined}
         */
        RelationPane.prototype.tick = function () {
            this.canvas.selectAll('.node')
                .attr("cx", function (d) {
                    return d.x;
                })
                .attr("cy", function (d) {
                    return d.y;
                });
            this.canvas.selectAll('.link')
                .attr("d", this.d);
        };

        return RelationPane;
    })();

    /**
     * Pageoptions Class
     * @type PageOptions
     */
    var PageOptions = (function () {

        /**
         * Constructor
         *
         * @returns {PageOptions}
         */
        var PageOptions = function () {

            this.dbSelect = $("#db");
            this.db = null;
            this.clearcacheInput = $('#clearcache');

            this.bibliographicrecordidInput = $('#bibliographicrecordid');
            this.nextBibliographicrecordid = null;
            this.agencyidSelect = $('#agencyid');
            this.nextAgencyid = null;
            this.onIdSelected = null;

            this.dbSelect.selectmenu({
                select: this.dbSelected.bind(this)
            });

            this.clearcacheInput.button();

            this.bibliographicrecordidInput.button();

            this.clearcacheInput.on('click', clearCache);

            this.bibliographicrecordidInput.on('change', this.bibliographicRecordIdChanged.bind(this));
            this.agencyidSelect.selectmenu({
                select: this.agencyidSelected.bind(this)
            });
            $(window).on('hashchange', this.hashChanged.bind(this));
            this.dbFetch();
        };

        /**
         * Clear widget content
         *
         * @returns {undefined}
         */
        PageOptions.prototype.clear = function () {
            this.nextAgencyid = null;
            this.nextBibliographicrecordid = null;
            this.dbClear();
            this.bibliographicrecordidInput.val('');
            this.agencyidClear();
        };

        /**
         * Set Id (key) for page
         *
         * @param {type} key
         * @returns {undefined}
         */
        PageOptions.prototype.setId = function (key) {
            var arg = key.split(",", 3);
            if (this.dbSelect.val() !== arg[0] ||
                this.bibliographicrecordidInput.val() !== arg[2] ||
                this.agencyidSelect.val() !== arg[1]) {
                this.nextDb = arg[0];
                this.nextBibliographicRecordId = arg[2];
                this.nextAgencyid = arg[1];

                this.dbAutoSelect();
            }
        };


        PageOptions.prototype.dbClear = function () {
            this.dbSelect.children().each(function (i) {
                if (i > 0)
                    $(this).remove();
            });
            this.dbSelect.selectmenu('refresh');
        };

        PageOptions.prototype.dbFetch = function () {
            ajax("resources/dbs", this.dbFetched.bind(this));
            return this;
        };

        PageOptions.prototype.dbFetched = function (data) {
            var that = this;
            data.forEach(function (e) {
                var option = $('<option/>');
                option.attr({value: e}).text(e);
                that.dbSelect.append(option);
            });
            this.dbSelect.selectmenu('refresh');
            if (data.length === 1) {
                this.dbSelect.prop('selectedIndex', 1);
                this.dbSelect.hide();
                this.dbSelect.selectmenu('refresh');
                this.dbSelected();
            } else {
                this.dbAutoSelect();
            }
        };

        PageOptions.prototype.dbAutoSelect = function () {
            var selected = false;
            if (this.nextDb !== null) {
                var i = $('#db option[value="' + this.nextDb + '"]').index();
                if (i > 0) {
                    this.dbSelect.prop('selectedIndex', i);
                    selected = true;
                    this.nextDb = null;
                }
            }
            this.dbSelect.selectmenu('refresh');
            if (selected)
                this.dbSelected();
            this.bibliographicRecordIdAutoChange();
        };

        PageOptions.prototype.dbSelected = function () {
            var newDb = this.dbSelect.val();
            if (this.db !== newDb) {
                this.db = newDb;
                if (this.nextBibliographicrecordid === null)
                    this.nextBibliographicrecordid = this.bibliographicrecordidInput.val();
                this.bibliographicRecordIdClear();
                if (this.nextAgencyid === null && this.agencyidSelect.val() !== '')
                    this.nextAgencyid = this.agencyidSelect.val();
                this.agencyidClear();
            }
            this.bibliographicRecordIdAutoChange();
        };
        /**
         * Internal: Callback, for changed record
         *
         * @returns {undefined}
         */
        PageOptions.prototype.bibliographicRecordIdClear = function () {
            this.bibliographicrecordidInput.val();
        };
        /**
         * Internal: Callback, for changed record
         *
         * @returns {undefined}
         */
        PageOptions.prototype.bibliographicRecordIdAutoChange = function () {
            if (this.nextBibliographicRecordId !== null) {
                this.bibliographicrecordidInput.val(this.nextBibliographicRecordId);
                this.bibliographicRecordIdChanged();
            }
        };
        /**
         * Internal: Callback, for changed record
         *
         * @returns {undefined}
         */
        PageOptions.prototype.bibliographicRecordIdChanged = function () {
            if (this.db === null)
                return;
            var bibliographicrecordid = this.bibliographicrecordidInput.val();
            this.agencyidClear();
            if (bibliographicrecordid !== '') {
                ajax("resources/agencies-with/" + this.db + "/" + bibliographicrecordid,
                    this.agencyidFetched.bind(this));
            }
        };

        /**
         * Internal: Callback for agencies-with ajax call
         *
         * @param {type} data
         * @returns {undefined}
         */
        PageOptions.prototype.agencyidFetched = function (data) {
            var that = this;
            if (this.nextAgencyid === null && this.agencyidSelect.val() !== '')
                this.nextAgencyid = this.agencyidSelect.val();
            this.agencyidClear();
            data.forEach(function (e) {
                var option = $('<option/>');
                option.attr({value: e}).text(e);
                that.agencyidSelect.append(option);
            });
            var selected = false;
            if (this.nextAgencyid !== null) {
                var i = $('#agencyid option[value="' + this.nextAgencyid + '"]').index();
                if (i > 0) {
                    this.agencyidSelect.prop('selectedIndex', i);
                    selected = true;
                }
            }
            this.nextAgencyid = null;
            this.agencyidSelect.selectmenu('refresh');
            if (selected) {
                this.agencyidSelected();
            }
        };

        /**
         * Internal: Clear agencies select
         *
         * @returns {undefined}
         */
        PageOptions.prototype.agencyidClear = function () {
            this.agencyidSelect.children().each(function (i) {
                if (i > 0)
                    $(this).remove();
            });
            this.agencyidSelect.selectmenu('refresh');
        };

        /**
         * Internal: Callback for agency selected
         *
         * @returns {undefined}
         */
        PageOptions.prototype.agencyidSelected = function () {
            var db = this.dbSelect.val();
            var bibliographicrecordid = this.bibliographicrecordidInput.val();
            var agencyid = this.agencyidSelect.val();
            if (bibliographicrecordid !== '' && agencyid !== '') {
                if (this.onIdSelected !== null)
                    this.onIdSelected(this.db + "," + agencyid + "," + bibliographicrecordid);
                document.location.hash = db + "," + agencyid + "," + bibliographicrecordid;
            }
        };

        /**
         * Set callback
         *
         * @param {type} onIdSelected
         * @returns {undefined}
         */
        PageOptions.prototype.setOnIdSelected = function (onIdSelected) {
            this.onIdSelected = onIdSelected;
        };

        /**
         * Internal: when window hash changes
         *
         * @returns {undefined}
         */
        PageOptions.prototype.hashChanged = function () {
            var hash = document.location.hash;
            if (hash !== '') {
                this.setId(hash.substr(1));
            }
        };

        /**
         * Start GlobalOptions logic
         *
         * @returns {undefined}
         */
        PageOptions.prototype.start = function () {
            this.hashChanged();
        };

        return PageOptions;
    })();


// Business logic object
    var displayPane = new DisplayPane();
    var relationPane = new RelationPane();
    var pageOptions = new PageOptions();

// displayPane.content.

// Business logic relations
    relationPane.setOnSelect(displayPane.setId.bind(displayPane));
    relationPane.setOnClick(pageOptions.setId.bind(pageOptions));
    pageOptions.setOnIdSelected(function (key) {
        displayPane.setId(key);
        relationPane.setId(key);
    });

// Tabs logic
    $("#recordTabs").tabs({
        beforeActivate: function (event, ui) {
            if (ui.oldTab.context.hash === '#recordTabs-relations') {
                relationPane.stop();
            } else if (ui.oldTab.context.hash === '#recordTabs-content') {
            }
        },
        activate: function (event, ui) {
            if (ui.newTab.context.hash === '#recordTabs-relations') {
                relationPane.resize();
                relationPane.start();
            } else if (ui.newTab.context.hash === '#recordTabs-content') {
                displayPane.resize();
            }
        }
    });

// Start
    displayPane.setId('');
    relationPane.setId('');
    displayPane.resize();
    pageOptions.clear();
    pageOptions.start();
})
;
