<!DOCTYPE html>
<meta charset="utf-8">
<style>

body {
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
  width: 960px;
  height: 500px;
  position: relative;
  }

svg {
	width: 100%;
	height: 100%;
}

path.slice{
	stroke-width:2px;
}

polyline{
	opacity: .3;
	stroke: black;
	stroke-width: 2px;
	fill: none;
}

</style>
<body>
<button class="randomize">randomize</button>

<script src="http://d3js.org/d3.v3.min.js"></script>


<script>

var svg = d3.select("body")
	.append("svg")
	.append("g")

svg.append("g")
	.attr("class", "slices");
svg.append("g")
	.attr("class", "labels");
svg.append("g")
	.attr("class", "lines");
var i = 0;
var width = 960,
    height = 450,
	radius = Math.min(width, height) / 2;

var pie = d3.layout.pie()
	.sort(null)
	.value(function(d) {
		return d.value;
	});

var arc = d3.svg.arc()
	.outerRadius(radius * 0.8)
	.innerRadius(radius * 0.4);

var outerArc = d3.svg.arc()
	.innerRadius(radius * 0.9)
	.outerRadius(radius * 0.9);

svg.attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

var key = function(d){ return d.data.label; };

var color = d3.scale.ordinal()
	.domain(["iPhone", "Android", "Client", "Instagram","iPad"])
	.range(["#a05d56", "#d0743c", "#ff8c00", "#98abc5"]);//, "#8a89a6", "#7b6888", "#6b486b "];

function randomData (){

var data = [	<?php
$connection = new MongoClient();
$collection = $connection->twitterspark->nfltweet;


$start = array(new MongoDate(strtotime("2015-11-30 01:48:49.000Z")),new MongoDate(strtotime("2015-11-30 02:40:49.000Z")));
$end = array(new MongoDate(strtotime("2015-11-30 02:40:49.000Z")),new MongoDate(strtotime("2015-11-30 03:40:49.000Z")));
for($x=0;$x<=1;$x++){

$iPhone[$x] = $collection->find(array('source'=>'iPhone','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$Android[$x] = $collection->find(array('source'=>'Android','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$Client[$x] = $collection->find(array('source'=>'Client','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$iPad[$x] = $collection->find(array('source'=>'iPad','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$Instagram[$x]= $collection->find(array('source'=>'Instagram','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$twitterfeed[$x]= $collection->find(array('source'=>'twitterfeed','date'=>array('$gt'=> $start[$x],'$lte'=>$end[$x])));
$Total[$x] = $collection->find();


$Others[$x] = ($Total[$x]->count())-(($Client[$x]->count())+($iPhone[$x]->count())+($Android[$x]->count())+($Instagram[$x]->count())+($iPad[$x]->count())+($twitterfeed[$x]->count()));
if($x==1){
echo $iPhone[$x]->count().",".$Android[$x]->count().",".$Client[$x]->count().",".$Instagram[$x]->count().",".$iPad[$x]->count();
}else{
echo $iPhone[$x]->count().",".$Android[$x]->count().",".$Client[$x]->count().",".$Instagram[$x]->count().",".$iPad[$x]->count().",";	
}
}
?>];

var labels = color.domain();
	return labels.map(function(label){
		i++;
		if(i == 14)
		{
			i=0;
			
			console.log(data[i+13]);
			return { label: label, value: data[i+13]}
		}
			else{
			
			console.log(data[i-1]);
		return { label: label, value: data[i-1] }
			}

		
	
	});
}

change(randomData());

d3.select(".randomize")
	.on("click", function(){
		change(randomData());
	});


function change(data) {
	
	

	/* ------- PIE SLICES -------*/
	var slice = svg.select(".slices").selectAll("path.slice")
		.data(pie(data), key);

	slice.enter()
		.insert("path")
		.style("fill", function(d) { return color(d.data.label); })
		.attr("class", "slice");

	slice		
		.transition().duration(1000)
		.attrTween("d", function(d) {
			this._current = this._current || d;
			var interpolate = d3.interpolate(this._current, d);
			this._current = interpolate(0);
			return function(t) {
				return arc(interpolate(t));
			};
		})

	slice.exit()
		.remove();

	/* ------- TEXT LABELS -------*/

	var text = svg.select(".labels").selectAll("text")
		.data(pie(data), key);

	text.enter()
		.append("text")
		.attr("dy", ".35em")
		.text(function(d) {
			return d.data.label;
		});
	
	function midAngle(d){
		return d.startAngle + (d.endAngle - d.startAngle)/2;
	}

	text.transition().duration(1000)
		.attrTween("transform", function(d) {
			this._current = this._current || d;
			var interpolate = d3.interpolate(this._current, d);
			this._current = interpolate(0);
			return function(t) {
				var d2 = interpolate(t);
				var pos = outerArc.centroid(d2);
				pos[0] = radius * (midAngle(d2) < Math.PI ? 1 : -1);
				return "translate("+ pos +")";
			};
		})
		.styleTween("text-anchor", function(d){
			this._current = this._current || d;
			var interpolate = d3.interpolate(this._current, d);
			this._current = interpolate(0);
			return function(t) {
				var d2 = interpolate(t);
				return midAngle(d2) < Math.PI ? "start":"end";
			};
		});

	text.exit()
		.remove();

	/* ------- SLICE TO TEXT POLYLINES -------*/

	var polyline = svg.select(".lines").selectAll("polyline")
		.data(pie(data), key);
	
	polyline.enter()
		.append("polyline");

	polyline.transition().duration(1000)
		.attrTween("points", function(d){
			this._current = this._current || d;
			var interpolate = d3.interpolate(this._current, d);
			this._current = interpolate(0);
			return function(t) {
				var d2 = interpolate(t);
				var pos = outerArc.centroid(d2);
				pos[0] = radius * 0.95 * (midAngle(d2) < Math.PI ? 1 : -1);
				return [arc.centroid(d2), outerArc.centroid(d2), pos];
			};			
		});
	
	polyline.exit()
		.remove();
};

</script>
</body>