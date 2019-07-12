package sg.bigo.presto.hive.parse;

import java.util.ArrayList;

/**
 * HiveParseException.
 *
 */
public class HiveParseException extends Exception {

  private static final long serialVersionUID = 1L;
  ArrayList<HiveParseError> errors;

  public HiveParseException(ArrayList<HiveParseError> errors) {
    super();
    this.errors = errors;
  }

  @Override
  public String getMessage() {

    StringBuilder sb = new StringBuilder();
    for (HiveParseError err : errors) {
      if (sb.length() > 0) {
        sb.append('\n');
      }
      sb.append(err.getMessage());
    }

    return sb.toString();
  }

}
