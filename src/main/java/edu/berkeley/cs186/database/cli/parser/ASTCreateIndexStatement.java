/* Generated By:JJTree: Do not edit this line. ASTCreateIndexStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.berkeley.cs186.database.cli.parser;

public
class ASTCreateIndexStatement extends SimpleNode {
  public ASTCreateIndexStatement(int id) {
    super(id);
  }

  public ASTCreateIndexStatement(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=761ebcb3adbae3fe55b9b1a7abf621f6 (do not edit this line) */
