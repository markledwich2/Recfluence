var linkOpacity = 0.3;

const vizEl = document.getElementById('viz');
const rect = vizEl.getBoundingClientRect();
const width = rect.width;
const height = rect.height;

loadChart();

async function loadChart() {
    const channels = await d3.csv("https://raw.githubusercontent.com/markledwich2/YouTubeNetworks/master/Data/3.Vis/Channels.csv");
    const recommends = await d3.csv("https://raw.githubusercontent.com/markledwich2/YouTubeNetworks/master/Data/3.Vis/ChannelRelations.csv");
    const links = recommends.map(d => new Link(d.FromChannelId, d.ChannelId, d.Size));
    const nodes = channels.map(d => new Node(d.id, d.title, d.subCount))

    const force = d3.forceSimulation(nodes)
        .force("charge", d3.forceManyBody())
        .force("center", d3.forceCenter())
        .force("link", d3.forceLink(links).id(d => d.id));

    for (i = 0; i < 100; i++)
        force.tick();

    var adjlist = new Map();
    links.forEach(d => {
        adjlist.set(d.source.id + "-" + d.target.id, true);
        adjlist.set(d.target.id + "-" + d.source.id, true);
    });

    function neigh(a, b) {
        return a == b || adjlist.get(a + "-" + b);
    }

    const svg = d3.select("#viz")
        .attr("viewBox", [-width / 2, -height / 2, width, height]);

    var container = svg.append("g");

    svg.call(
        d3.zoom()
            .scaleExtent([.1, 4])
            .on("zoom", () => container.attr("transform", d3.event.transform))
    );

    var link = container.append("g").attr("class", "links")
        .selectAll("line")
        .data(links)
        .enter()
        .append("line")
        .attr("class", "link")
        .attr("stroke-width", d => d.value);

    var node = container.append("g").attr("class", "nodes")
        .selectAll("g")
        .data(nodes)
        .enter().append("g")
        .attr("class", "node");

    node.append("circle")
        .attr("r", d => Math.sqrt(d.subs) / 300);
    //.attr("fill", function (d) { return color(d.group); })

    node.append("text")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(d => d.title);

    node.on("mouseover", focus).on("mouseout", unfocus);

    force.on("tick", () => {
        node.call(updateNode);
        link.call(updateLink);
    });


    function focus(d) {
        var id = d.id;
        node.style("opacity", function (o) {
            return neigh(id, o.id) ? 1 : 0.1;
        });
        link.style("opacity", function (o) {
            return o.source.id == id || o.target.id == id ? linkOpacity : 0.1;
        });
    }

    function unfocus() {
        node.style("opacity", 1);
        link.style("opacity", linkOpacity);
    }

    function fixna(x) {
        if (isFinite(x)) return x;
        return 0;
    }

    function updateLink(link) {
        link.attr("x1", function (d) { return fixna(d.source.x); })
            .attr("y1", function (d) { return fixna(d.source.y); })
            .attr("x2", function (d) { return fixna(d.target.x); })
            .attr("y2", function (d) { return fixna(d.target.y); });
    }

    function updateNode(node) {
        node.attr("transform", function (d) {
            return "translate(" + fixna(d.x) + "," + fixna(d.y) + ")";
        });
    }
}

class Link {
    constructor(source, target, value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }
}

class Node {
    constructor(id, title, subs) {
        this.id = id;
        this.title = title;
        this.subs = subs;
    }
}
