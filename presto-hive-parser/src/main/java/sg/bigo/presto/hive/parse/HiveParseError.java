package sg.bigo.presto.hive.parse;

import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.RecognitionException;

/**
 *
 */
public class HiveParseError {
  private final BaseRecognizer br;
  private final RecognitionException re;
  private final String[] tokenNames;

  HiveParseError(BaseRecognizer br, RecognitionException re, String[] tokenNames) {
    this.br = br;
    this.re = re;
    this.tokenNames = tokenNames;
  }

  BaseRecognizer getBaseRecognizer() {
    return br;
  }

  RecognitionException getRecognitionException() {
    return re;
  }

  String[] getTokenNames() {
    return tokenNames;
  }

  String getMessage() {
    return br.getErrorHeader(re) + " " + br.getErrorMessage(re, tokenNames);
  }

}
