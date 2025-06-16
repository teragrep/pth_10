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
package com.teragrep.pth10.ast.bo;

import java.util.ArrayList;
import java.util.List;

/**
 * Base abstract class for all Nodes
 */
public abstract class Node {

    com.teragrep.pth10.ast.bo.Token token;
    List<Node> children;

    public Node() {
        ; // disconnected ones for lists etc
    }

    public Node(com.teragrep.pth10.ast.bo.Token token) {
        this.token = token;
    }

    // Convenience API to get token base type
    public Token.Type getNodeType() {
        return token.getType();
    }

    public void addChild(Node child) {
        if (this.children == null) {
            this.children = new ArrayList<Node>();
        }
        this.children.add(child);
    }

    public List<Node> getChildren() {
        return this.children;
    }

    public String toString() {
        if (this.token != null) {
            return this.token.toString();
        }
        else {
            return "null";
        }
    }

    public String toTree() {
        if (children != null && children.size() > 0) {
            // recurse children
            StringBuilder subTree = new StringBuilder();

            subTree.append("{");
            subTree.append(this.toString());
            subTree.append(" ");

            // their
            int nChild = this.children.size();
            while (nChild > 0) {
                Node child = children.get(nChild - 1);
                subTree.append(" ");
                subTree.append(child.toTree());
                nChild--;
            }

            subTree.append("}");

            return subTree.toString();
        }
        else {
            // leaf
            return this.toString();
        }
    }

    public String toXMLTree() {
        if (children != null && children.size() > 0) {
            // recurse children
            StringBuilder subTree = new StringBuilder();

            subTree.append("<");
            subTree.append(this.toString());
            subTree.append(" ");

            // their
            int nChild = this.children.size();
            while (nChild > 0) {
                Node child = children.get(nChild - 1);
                subTree.append(" ");
                subTree.append(child.toTree());
                nChild--;
            }

            subTree.append(">");

            return subTree.toString();
        }
        else {
            // leaf
            return this.toString();
        }
    }
}
