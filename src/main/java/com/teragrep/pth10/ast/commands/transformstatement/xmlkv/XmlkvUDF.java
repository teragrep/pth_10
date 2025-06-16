/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth10.ast.commands.transformstatement.xmlkv;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class XmlkvUDF implements UDF1<String, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlkvUDF.class);

    @Override
    public Map<String, String> call(String input) throws Exception {
        Map<String, String> m = new HashMap<>();
        try {
            Document doc = DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder()
                    .parse(new InputSource(new StringReader(input)));
            Node n = doc.getDocumentElement();
            buildMapFromXmlNodes(n, m);

        }
        catch (SAXParseException spe) {
            // don't catch other than parse errors
            LOGGER
                    .warn(
                            "Could not parse col <{}> on line <{}>, returning empty.", spe.getColumnNumber(),
                            spe.getLineNumber()
                    );
        }

        return m;
    }

    /**
     * Gets all latest occurrences of tag-contents pairs <pre>deepest node => its contents</pre>
     * 
     * @param rootNode root node (Main Document Element)
     * @param map      Final map to be returned out of the UDF
     */
    private void buildMapFromXmlNodes(final Node rootNode, final Map<String, String> map) {
        // RootNode is text
        if (rootNode.getNodeName().equals("#text")) {
            // the parent node of a #text node is the visible tag in XML string, e.g. <a>text</a> would be "a"
            String colName = rootNode.getParentNode().getNodeName();
            // latest occurrence always takes the priority
            map.put(colName, rootNode.getTextContent());
        }

        final NodeList nl = rootNode.getChildNodes();
        // Visit children of current node
        for (int i = 0; i < nl.getLength(); i++) {
            Node child = nl.item(i);
            buildMapFromXmlNodes(child, map);
        }
    }
}
