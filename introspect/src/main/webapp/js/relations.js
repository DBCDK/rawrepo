var SVGRelations = (function () {

    /**
     * Fallback for console.log
     * 
     * @returns {undefined}
     */
    var LOG = function () {
    };
    if (console && console.log)
        LOG = console.log;

    var ERROR = function (text) {
        var d = $('<div class="ui-button ui-widget ui-corner-all ui-state-default"/>');
        d.text(text);
        $('#errors').append(d);
        setTimeout(function () {
            d.remove();
        }, 3000);
    };


    var linkDistance = {
        parent: 50,
        sibling: 15
    };

    /**
     * Setup all d3 statics for a force layout
     * 
     * @param {type} selector
     * @returns {SVGRelations}
     */
    var SVGRelations = function (selector) {
        this.color = {
            nodeSelected: 'red',
            nodeUnexpanded: 'blue',
            nodeDeselected: 'green',
            linkParent: 'orangered',
            linkSibling: 'purple',
            unknown: 'white'
        };
        this.selected = '';
        this.collection = {};
        this.onSelect = null;
        this.onHover = null;
        this.onClick = null;
        this.width = 500;
        this.height = 300;
        this.domParent = d3.select(selector);
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
                    return (linkDistance[d.type]) * factor;
                })
                .friction(.3)
                .gravity(0.00)
                .charge(-100)
                .on("tick", this.tick.bind(this));
        this.canvas.append("svg:defs")
                .selectAll("marker")
                .data(["end"])                   // Different link/path types can be defined here
                .enter().append("svg:marker")    // This section adds in the arrows
                .attr("id", String)
                .attr("viewBox", "0 -5 10 10")
                .attr("refX", 15)
                .attr("refY", -1.5)
                .attr("markerWidth", 6)
                .attr("markerHeight", 6)
                .attr("orient", "auto")
                .append("svg:path")
                .attr("d", "M0,-5L10,0L0,5");
        this.resize();
    };

    /**
     * Set data collection
     *  
     * @param {type} collection
     * @returns {undefined}
     */
    SVGRelations.prototype.setCollection = function (collection) {
        this.selected = null;
        this.collection = collection;
        this.canvas.selectAll('.node').remove();
        this.canvas.selectAll('.link').remove();
        this.refresh();
    };

    /**
     * refresh force layout, for instance if properties has changed
     * 
     * @returns {undefined}
     */
    SVGRelations.prototype.refresh = function () {
        this.force
                .nodes(this.collection.nodes)
                .links(this.collection.links)
                .start();

        this.canvas.selectAll('.link')
                .data(this.force.links())
                .enter()
                .append('path')
                .attr('class', 'link')
                .attr("marker-end", "url(#end)")
                .style('fill', 'none')
                .style('stroke', function (d) {
                    return d.type === 'parent' ? this.color.linkParent :
                            d.type === 'sibling' ? this.color.linkSibling :
                            this.color.unknown;
                }.bind(this))
                .style('stroke-width', 1);

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
                    return d.content === null ? this.color.nodeUnexpanded :
                            this.color.nodeDeselected;
                }.bind(this))
                .on('mouseover', function (d) {
                    if (this.onSelect !== null && this.selected === d.key) {
                        return;
                    }
                    if (this.onHover !== null)
                        this.onHover(d.key);
                }.bind(this))
                .on('mouseout', function (d) {
                    if (this.onHover !== null)
                        this.onHover(null);
                }.bind(this))
                .on('click', function (d) {
                    if (d3.event.defaultPrevented)
                        return; // ignore drag
                    if (this.onSelect !== null)
                        this.onSelect(d.key);
                    if (d.content === null) {
                        var args = d.key.split(',', 2);
                        this.collection.load(args[0], args[1]);
                    }
                    this.setHighlight(d.key);
                }.bind(this))
                .on('dblclick', function (d) {
                    if (this.onClick !== null) {
                        var args = d.key.split(',', 2);
                        this.onClick(args[0], args[1]);
                    }
                }.bind(this))
                .call(this.force.drag);

        if (this.selected === null &&
                this.collection.nodes.length > 0) {
            this.setHighlight(this.collection.nodes[0].key);
        }
    };

    /**
     * Resize canvas
     * @returns {undefined}
     */
    SVGRelations.prototype.resize = function () {
        var style = window.getComputedStyle(this.domParent[0][0]);
        var width = Math.max(parseInt(style.width) - 32, 500);
        var height = Math.max(parseInt(style.height) - 32, 300);
        if (this.width !== width || this.height !== height) {
            this.width = width;
            this.height = height;
            this.svg.attr("width", this.width)
                    .attr("height", this.height);
            this.force.size([this.width, this.height]);
            this.force.start();
        }
    };
    SVGRelations.prototype.start = function () {
        this.force.start();
    };

    SVGRelations.prototype.stop = function () {
        this.force.stop();
    };

    SVGRelations.prototype.setHighlight = function (selected) {
        if (this.selected !== selected) {
            if (this.onSelect !== null) {
                this.onSelect(selected);
                if (this.onHover !== null) {
                    this.onHover(null);
                }
            }
            this.selected = selected;

            var t = d3.transition()
                    .duration(1000);
            t.selectAll('.node')
                    .style('fill', function (d, i) {
                        return i === 0 ? this.color.nodeSelected :
                                d.content === null && d.key !== this.selected ? this.color.nodeUnexpanded :
                                this.color.nodeDeselected;
                    }.bind(this))
                    .attr('r', function (d) {
                        return d.key === this.selected ? 10 : 5;
                    }.bind(this))
                    .call(this.force.start);
        }
    };
    SVGRelations.prototype.setOnSelect = function (onSelect) {
        this.onSelect = onSelect;
    };
    SVGRelations.prototype.setOnHover = function (onHover) {
        this.onHover = onHover;
    };
    SVGRelations.prototype.setOnClick = function (onClick) {
        this.onClick = onClick;
    };
    SVGRelations.prototype.d = function (d) {
        var dx = d.target.x - d.source.x,
                dy = d.target.y - d.source.y,
                dr = Math.sqrt(dx * dx + dy * dy),
                sr = d.source.node.attr('r'),
                tr = d.target.node.attr('r'),
                sx = d.source.x + (dx * sr / dr),
                sy = d.source.y + (dy * sr / dr),
                tx = d.target.x - (dx * tr / dr),
                ty = d.target.y - (dy * tr / dr);
        return "M" + sx + "," + sy + "A" + dr + "," + dr + " 0 0,1 " + tx + "," + ty;
    };
    SVGRelations.prototype.tick = function () {
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
    return SVGRelations;
})();