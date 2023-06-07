<h1>DPL-Parser</h1>
<p>This package contains DPL-Parser and related walkers for generating different queries for Spark and Archive.
</p>
<h2>Parts</h2>
<p>Whole package consist of lexer and parse, packed under ast-directory and post processors packed under walker directory.
in addition to that, jooq.generated contains meta-models used with ConditionWalker</p>
<h3>Visitors</h3>
<p>Visitor are that part of the DPL parser which generates either SparkSQL- or XML-representation from incoming DPL-query</p>
<ol>
<li>DPLParserBaseVisitorImpl transforms parse-tree into the SparkSQL-queries.</li>
<li>DPLParserXMLVisitor transforms parse-tree into the XML-representation. That is later on transformed into the 
Conditions or Columns using appropriate <i>walker</i>.</li>
</ol>
<h3>Walkers</h3>
<ol>
<li><p>XmlWalker is a base class which individual walkers inherit</p>
<p>- Abstract class</p>
<p>- Contains traverse method which walks XML-tree and for each node calls emit-method which each implementing walker must provide</p>
<p>- Can't be used alone but concrete implementation must be provided</p>
</li>
<li><p>ConditionWalker is implementation returning Spark Condition objects which are used for Archive queries</p>
<p>- Uses only LogicalStatement part so does not implement full DPL-syntax, only subset used in logical statements.</p></li>
<li><p>DataframeWalker is implementation returning Spark dataframe-manipulation pipeline which can be used for manipulating Data streams from Kafka and Archive</p>
<p>- Should handle full DPL-syntax</p>
<p>- Consist of main DataframeWalker-class and static emitters under <i>dataframewalker directory</i>.</p>
<p>- DataframeWalkerImpl selects which emitter to call when walker travels through XML-document</p>
</li></ol>
<h2>How to use different parsers and Walkers</h2>
<p>Here is an example of how parsing stack works together. Following code sample illuminates how it works.</p>
<blockquote>

```java
   String q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";
   CharStream inputStream = CharStreams.fromString(q);
   DPLLexer lexer = new DPLLexer(inputStream);
   DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
   ParseTree tree = parser.root();
   // Parse incoming DPL-query 
   DPLParserXMLVisitor visitor = new DPLParserXMLVisitor("-1Y",null, q);
   fullResult = visitor.visit(tree).toString();
   // get logical part which is used for archive queries
   // ConditionWalker is used for that.     
   logicalPart = visitor.getLogicalPart();
   
   // check column for archive query i.e. only logical part
   // This is used in Archive query
   String r = conditionWalker.fromString(logicalPart,false).toString();

   // Full query generates Column-object
   DataFrame<Row> result = dataframeWalker.fromString(fullResult);
```
</blockquote>
<ol>
<li>Lexer analyze incoming DPL-stream</li>
<li>DPL-parser takes token stream and generates parse-tree</li>
<li>XmlVisitor transforms that parse-tree into the XML-document</li>
<li>ConditionWalker transforms query to Archive using XML-tree as a source</li>
<li>DataframeWalker transforms query for Spark executable processing pipes using XML-tree as a source</li>
</ol>
<h2>XML-sample</h2>
<p>Here is a resulting XML-document from simple DPL-query. That XML-tree is then uses as a source for Condition- and ColumnWalkers</p>
<p>

```xml

<root>
    <!--index = voyager _index_earliest="04/16/2020:10:25:40" | chart count(_raw) as count by _time | where  count > 70-->
    <search root="true">
        <logicalStatement>
            <AND>
                <index operation="EQUALS" value="voyager"/>
                <index_earliest operation="GE" value="1587021940"/>
            </AND>
            <transformStatements>
                <transform>
                    <divideBy field="_time">
                        <chart field="_raw" fieldRename="count" function="count">
                            <transform>
                                <where>
                                    <evalCompareStatement field="count" operation="GT" value="70"/>
                                </where>
                            </transform>
                        </chart>
                    </divideBy>
                </transform>
            </transformStatements>
        </logicalStatement>
    </search>
</root>
```

<h2>Source code structure</h2>
<p>Code structure reflects rather directly parser tree structure. Recognized parts are limited usually in their own classes.
Here is a existing  directory structure with little explanations.</p>
<h3>ast</h3>
<p>contains language related classes.</p>
<ol>
    <li><b><i>bo</i></b> contains value objects which are used during DPL->Catalyst transformation to pass values forward.
Notable classes are <i>StringNode, CatalystNode, ColumnNode and SubSearchNode.</i>
    </li>
<li><b><i>commands</i></b> contains several visitor implementations their support classes and different context manipulation
items. Individual commands are collected in their own classes or even packages.</li>
<ul><b><i>aggregate</i></b> contains aggregate commands and their UDF-function implementations</ul>
<ul><b><i>evalstatement</i></b> contains eval functions, their  UDF-implementations and actual EvalStatement which acts as a 
main selector which handles all recognized eval operations</ul>
<ul><b><i>logicalstatement</i></b> contains logicalStatement handling. In addition to that, there is also TimeStatement for handling 
time-ranges and dates.</ul>
<ul><b><i>transformstatement</i></b> handles different transformations commands. Each transformation is implemented as separate class.
Actual integration point is <i>TransformStatement</i> which selects which  transformation to call and pass results back.</ul>
</ol>
<h4>Visitors, contexts and utils</h4>
<p>Visitors manages different DPL->target language transformations. Currently there is DPL->XML transformation which is
used  when executing Archive queries and  DPL->Catalyst transformation which is run in Spark-cluster for Archive and Kafka
streams.</p>
<p>DPLCatalystContext is used for passing data between parser and  runtime environment. It contains for instance
DPLParserConfig and DPLAuditInformation. Main point is that all configuration and runtime-relates  data should pe passed 
through it because it is passed  through visitors and  each transformation and command should have access to it.</p>
<p>There are several utils which manages for instance timestamp->epoch manipulations, general utils like quote-stripping, 
some debugging information tools and time-calculation  utils.</p>
<p>ProcessingStack contains  current DataSet which visitor constructs. Apart from that if offers access to Parallel and  Sequential 
stacks which can be manipulated. UI cal ask  stack from visitor and resolve  whether it is  serial and act differently 
according to results. Usage samples can be found in StackTest</p>
<h3>datasource</h3>
<p><i>datasource</i> contains 2 datasource implementations.</p>
<ol>
<li><b><i>DPLDatasource</i></b> handles Archive connection and pulls out data from the according to query.</li>
<li><b><i>GeneratedDatasource</i></b> offers way to return line or lines as a spark in-memory dataframes. That functionality is
utilized in parser state commands like dpl, explain and teragrep.</li>
</ol>
