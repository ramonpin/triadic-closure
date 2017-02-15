package pin.ramon.hadoop.grafos

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ByteWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import java.util.*


/**
 * TriadicClosureMapper
 * Created by ramon.pin on 9/02/17.
 */
class TriadicClosureMapper : Mapper<LongWritable, Text, NodeWritable, NodeWritable>() {

    override public fun map(key: LongWritable, value: Text, context: Context) {
        val nodePair = NodePairWritable.Companion.fromString(value.toString())
        context.write(nodePair.node1, nodePair.node2)
        context.write(nodePair.node2, nodePair.node1)
    }

}

/**
 * TriadicClosureReducer
 * Created by ramon.pin on 9/02/17.
 */
class TriadicClosureReducer : Reducer<NodeWritable, NodeWritable, Text, Text>() {

    override public fun reduce(key: NodeWritable, values: Iterable<NodeWritable>, context: Context) {

        val nodes = ArrayList<NodeWritable>()
        values.forEach { current -> nodes.add(NodeWritable(current)) }


        nodes.forEachIndexed { i, currentNode ->
            (i + 1 .. nodes.size - 1).forEach { j ->
                if(currentNode < nodes[j]) {
                    context.write(Text(currentNode.toString()), Text(nodes[j].toString()))
                } else {
                    context.write(Text(nodes[j].toString()), Text(currentNode.toString()))
                }
            }
        }

    }

}

/**
 * DuplicatedPairRemoverMapper
 * Created by ramon.pin on 14/02/17
 */
class DuplicatedPairRemoverMapper : Mapper<LongWritable, Text, NodePairWritable, ByteWritable>() {

    companion object {
        const val nodesFile   : Byte = 1
        const val triadicFile : Byte = 2
    }

    private var nodesFilePath: String? = null

    override fun setup(context: Context) {
        nodesFilePath = context.configuration["triadic.file.nodes"]
    }

    override fun map(key: LongWritable, value: Text, context: Context) {
        val filePathString = (context.inputSplit as FileSplit).path.toString()
        if(filePathString == nodesFilePath) {
            context.write(NodePairWritable.fromString(value.toString()), ByteWritable(nodesFile))
        } else {
            context.write(NodePairWritable.fromString(value.toString()), ByteWritable(triadicFile))
        }
    }

}

/**
 * DuplicatedPairRemoverReducer
 * Created by ramon.pin on 14/02/17
 */
class DuplicatedPairRemoverReducer : Reducer<NodePairWritable, ByteWritable, Text, IntWritable>() {

    override fun reduce(key: NodePairWritable, values: Iterable<ByteWritable>, context:  Context) {

        val allValues = values.map(ByteWritable::get)
        val inNodesFile = allValues.contains(DuplicatedPairRemoverMapper.nodesFile)
        if(!inNodesFile) {
            context.write(Text(key.node1.toString() + "\t" + key.node2.toString()), IntWritable(allValues.count()))
        }

    }

}

/**
 * TriadicClosureDriver
 * Created by ramon.pin on 10/02/17.
 */
class TriadicClosureDriver : Configured(), Tool {

    override fun run(args: Array<out String>): Int {
        return if(triadicCalculation() && duplicatesRemover()) 0 else 1
    }

    private fun triadicCalculation(): Boolean {

        val job = Job.getInstance(conf, "TriadicClosureJob - Triangles")
        job.setJarByClass(this.javaClass)

        TextInputFormat.addInputPath(job, Path(conf["triadic.file.nodes"]))
        TextOutputFormat.setOutputPath(job, Path(conf["triadic.file.triagles"]))

        job.mapperClass         = TriadicClosureMapper().javaClass
        job.reducerClass        = TriadicClosureReducer().javaClass
        job.mapOutputKeyClass   = NodeWritable().javaClass
        job.mapOutputValueClass = NodeWritable().javaClass
        job.outputKeyClass      = Text().javaClass
        job.outputValueClass    = Text().javaClass

        job.submit()

        return job.waitForCompletion(true)
    }

    private fun duplicatesRemover(): Boolean {
        val job = Job.getInstance(conf, "TriadicClosureJob - Remove Duplicates")
        job.setJarByClass(this.javaClass)

        TextInputFormat.addInputPath(job,   Path(conf["triadic.file.nodes"]))
        TextInputFormat.addInputPath(job,   Path(conf["triadic.file.triagles"]))
        TextOutputFormat.setOutputPath(job, Path(conf["triadic.file.relations"]))

        job.mapperClass         = DuplicatedPairRemoverMapper().javaClass
        job.reducerClass        = DuplicatedPairRemoverReducer().javaClass
        job.mapOutputKeyClass   = NodePairWritable().javaClass
        job.mapOutputValueClass = ByteWritable().javaClass
        job.outputKeyClass      = Text().javaClass
        job.outputValueClass    = IntWritable().javaClass

        job.submit()

        return job.waitForCompletion(true)
    }

}

/**
 * TriadicClosureJob
 * Created by ramon.pin on 9/02/17.
 */
object TriadicClosureJob  {

    private operator fun CommandLine.get(name: String) : String = this.getOptionValue(name)

    @JvmStatic fun parseArgs(args: Array<String>): Configuration? {

        val ops = Options()
        ops.addOption("n", "nodes",     true, "Fichero que contiene los nodos del grafo o grafos a tratar.")
        ops.addOption("t", "triangles", true, "Fichero que contiene triangulos encontrados.")
        ops.addOption("r", "relations", true, "Fichero que contiene las relaciones deducidas.")

        val cmd = GnuParser().parse(ops, args)
        if(cmd.hasOption("nodes") && cmd.hasOption("triangles") && cmd.hasOption("relations")) {
            val conf = Configuration()
            conf["triadic.file.nodes"]     = cmd["nodes"]
            conf["triadic.file.triagles"]  = cmd["triangles"]
            conf["triadic.file.relations"] = cmd["relations"]
            return conf
        } else {
            HelpFormatter().printHelp("triadic-closure", ops)
            return null
        }

    }

    @JvmStatic fun main(args: Array<String>) {
        val conf = parseArgs(args)
        if(conf != null) {
            val fs = FileSystem.get(conf)
            fs.delete(Path(conf["triadic.file.triagles"]), true)
            fs.delete(Path(conf["triadic.file.relations"]), true)
            System.exit(ToolRunner.run(conf, TriadicClosureDriver(), args))
        } else {
            System.exit(1)
        }
    }
}
