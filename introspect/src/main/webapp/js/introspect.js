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
     * Ajax error function
     * 
     * @param {type} jqXHR
     * @param {type} textStatus
     * @param {type} errorThrown
     * @returns {undefined}
     */
    function ajaxError(jqXHR, textStatus, errorThrown) {
        ERROR(errorThrown);
    }

    /**
     * Create key(string) from composite
     * 
     * @param {type} id
     * @returns {String}
     */
    function record2Key(id) {
        return id.agencyid + "," + id.bibliographicrecordid;
    }

    /**
     * 
     * @param {type} s
     * @returns {introspect_L2.keyRecord.introspectAnonym$0}
     */
    function key2Record(s) {
        var a = s.match(/^(\d+),(.*)$/);
        if (a !== null) {
            return {
                agencyid: a[1],
                bibliographicrecordid: a[2]
            };
        }
        return null;
    }

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
            var that = this;
            this.idInput = $('#record-key');
            this.oldId = null;
            this.versionSelect = $('#version');
            this.otherVersionSelect = $('#other-version');
            this.content = $('#content');
            var versionSelected = that.versionSelected.bind(that);
            this.versionSelect.selectmenu({select: versionSelected});
            this.otherVersionSelect.selectmenu({select: versionSelected});

            $(window).on('resize', this.resize.bind(this));

            return this;
        };

        /**
         * Internal when a version or other version, has been selected
         * 
         * @param {type} event
         * @param {type} ui
         * @returns {undefined}
         */
        DisplayPane.prototype.versionSelected = function (event, ui) {
            var that = this;
            var key = this.idInput.val();
            var record = key2Record(key);
            var thisVersion = this.versionSelect.val();
            var otherVersion = this.otherVersionSelect.val();
            if (thisVersion === '')
                return;
            if (thisVersion === '-1') {
                this.otherVersionSelect.selectmenu('disable');
                $.ajax({
                    type: "GET",
                    dataType: "json",
                    url: "resources/record-merged/" + record.agencyid + "/" + record.bibliographicrecordid,
                    success: function (data, textStatus, jqXHR) {
                        that.displayRecord(data);
                    },
                    error: ajaxError
                });
            } else {
                this.otherVersionSelect.selectmenu('enable');
                if (otherVersion === '') {
                    $.ajax({
                        type: "GET",
                        dataType: "json",
                        url: "resources/record-historic/" + record.agencyid + "/" + record.bibliographicrecordid + "/" + thisVersion,
                        success: function (data, textStatus, jqXHR) {
                            that.displayRecord(data);
                        },
                        error: ajaxError
                    });
                } else {
                    $.ajax({
                        type: "GET",
                        dataType: "json",
                        url: "resources/record-diff/" + record.agencyid + "/" + record.bibliographicrecordid + "/" + thisVersion + "/" + otherVersion,
                        success: function (data, textStatus, jqXHR) {
                            that.displayRecord(data);
                        },
                        error: ajaxError
                    });
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
            var that = this;
            if (this.oldId !== val) {
                this.idInput.val(val);
                var record = key2Record(val);
                this.clear();
                if (record !== null) {
                    $.ajax({
                        type: "GET",
                        dataType: "json",
                        url: "resources/record-history/" + record.agencyid + "/" + record.bibliographicrecordid,
                        success: function (data, textStatus, jqXHR) {
                            that.historyRead(data);
                            that.oldId = val;
                        },
                        error: ajaxError
                    });
                    $.ajax({
                        type: "GET",
                        dataType: "json",
                        url: "resources/relations/" + record.agencyid + "/" + record.bibliographicrecordid,
                        success: function (data, textStatus, jqXHR) {
                            that.relationsRead(data);
                        },
                        error: ajaxError
                    });
                }
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
                    option.attr({value: i}).text(e['modified']);
                    $('#version').append(option);
                    tag.append(option);
                });
                tag.selectmenu('refresh');
                tag.selectmenu('enable');
            });
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

        /**
         * Resize content area to fit screen
         * 
         * @returns {undefined}
         */
        DisplayPane.prototype.resize = function () {
            var container = document.getElementById('content-container');
            var bodyHeight = parseInt(window.getComputedStyle(document.body).height);
            var windowHeight = window.innerHeight;
            if (bodyHeight > windowHeight || windowHeight - bodyHeight > 20) {
                var height = parseInt(window.getComputedStyle(container).height);
                height = Math.max(height - (bodyHeight - windowHeight), 300);
                height = height - 25;
                container.style.height = height + "px";
                container.style.width = "100%";
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
            linkParent: 'orangered',
            linkSibling: 'purple',
            unknown: 'white'
        };

        /**
         * Constructor
         * 
         * @returns {take2_L237.RelationPane}
         */
        var RelationPane = function () {
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
                        return (LINKDISTANCE[d.type]) * factor;
                    })
                    .friction(.3)
                    .gravity(0.00)
                    .charge(-250)
                    .on("tick", this.tick.bind(this));
            
            var resize = this.resize.bind(this);
            $(window).on('resize', resize);
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
            this.fetchRelations(key);
        };

        /**
         * Internal: Fetch relations for id (key)
         * 
         * @param {type} key
         * @returns {undefined}
         */
        RelationPane.prototype.fetchRelations = function (key) {
            var that = this;
            if (key in this.collection && this.collection[key].content !== null)
                return;
            var record = key2Record(key);
            if (record !== null) {
                $.ajax({
                    type: "GET",
                    dataType: "json",
                    url: "resources/relations/" + record.agencyid + "/" + record.bibliographicrecordid,
                    success: function (data, textStatus, jqXHR) {
                        that.relationsFetched(data);
                    },
                    error: ajaxError
                });
            }
        };

        /**
         * Internal: Callback for relations ajax call
         * 
         * @param {type} record
         * @returns {undefined}
         */
        RelationPane.prototype.relationsFetched = function (record) {
            var key = record2Key(record['me']);
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
                var key = record2Key(e);
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
                        return d.type === 'parent' ? COLOR.linkParent :
                                d.type === 'sibling' ? COLOR.linkSibling :
                                COLOR.unknown;
                    }.bind(this))
                    .style('stroke', function (d) {
                        return d.type === 'parent' ? COLOR.linkParent :
                                d.type === 'sibling' ? COLOR.linkSibling :
                                COLOR.unknown;
                    }.bind(this))
                    .style('stroke-width', 2);

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
                    .on('click', function (d) {
                        if (d3.event.defaultPrevented)
                            return; // ignore drag
                        if (this.onSelect !== null)
                            this.onSelect(d.key);
                        if (d.content === null)
                            this.fetchRelations(d.key);
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

        /**
         * Resize canvas according to parent size
         * 
         * @returns {undefined}
         */
        RelationPane.prototype.resize = function () {
            var style = window.getComputedStyle(this.domParent[0][0]);
            var width = Math.max(parseInt(style.width), 500);
            var height = Math.max(parseInt(style.height), 300);
            if (this.width !== width || this.height !== height) {
                this.width = width;
                this.height = height;
                this.svg.attr("width", this.width)
                        .attr("height", this.height);
                this.force.size([this.width, this.height]);
                this.force.start();
            }
        };

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
                    l = 8;
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
            var that = this;
            this.bibliographicrecordidInput = $('#bibliographicrecordid');
            this.oldBibliographicrecordid = null;
            this.agencyidSelect = $('#agencyid');
            this.oldAgencyid = null;
            this.nextAgencyid = null;

            this.onIdSelected = null;

            this.bibliographicrecordidInput.button();

            this.bibliographicrecordidInput.on('change', this.bibliographicrecordidChanged.bind(this));
            this.agencyidSelect.selectmenu({
                select: this.agencyidSelected.bind(this)
            });
            $(window).on('hashchange', this.hashChanged.bind(this));

            return this;
        };

        /**
         * Clear widget content
         * 
         * @returns {undefined}
         */
        PageOptions.prototype.clear = function () {
            this.oldBibliographicrecordid = null;
            this.oldAgencyid = null;
            this.nextAgencyid = null;
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
            var record = key2Record(key);
            this.nextAgencyid = record.agencyid;
            this.bibliographicrecordidInput.val(record.bibliographicrecordid);
            this.bibliographicrecordidChanged();
        };

        /**
         * Internal: Callback, for changed record
         * 
         * @returns {undefined}
         */
        PageOptions.prototype.bibliographicrecordidChanged = function () {
            var that = this;
            var bibliographicrecordid = this.bibliographicrecordidInput.val();
            var agencyid = this.agencyidSelect.val();
            if (this.oldBibliographicrecordid !== bibliographicrecordid ||
                    (this.nextAgencyid !== null && this.nextAgencyid !== agencyid)) {
                this.agencyidClear();
                if (bibliographicrecordid !== '') {
                    $.ajax({
                        type: "GET",
                        dataType: "json",
                        url: "resources/agencies-with/" + bibliographicrecordid,
                        success: function (data, textStatus, jqXHR) {
                            that.oldBibliographicrecordid = bibliographicrecordid;
                            that.agencyidFetched(data);
                        },
                        error: ajaxError
                    });
                }
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
                this.nextAgencyid = null;
            }
            this.agencyidSelect.selectmenu('refresh');
            if (selected)
                this.agencyidSelected();
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
            var bibliographicrecordid = this.bibliographicrecordidInput.val();
            var agencyid = this.agencyidSelect.val();
            if (bibliographicrecordid !== '' && agencyid !== '') {
                this.oldAgencyid = agencyid;
                if (this.onIdSelected !== null)
                    this.onIdSelected(agencyid + "," + bibliographicrecordid);
                document.location.hash = agencyid + "," + bibliographicrecordid;
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
                hash = hash.substr(1);
                this.setId(hash);
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
});
