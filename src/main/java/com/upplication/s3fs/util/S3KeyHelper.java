package com.upplication.s3fs.util;

import static com.upplication.s3fs.S3Path.PATH_SEPARATOR;

import java.util.List;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class S3KeyHelper {
	public static String[] getParts(String key) {
		String[] parts = key.split(PATH_SEPARATOR);
		String[] split = new String[parts.length];
		int i=0;
		for (String part : parts) {
			try {
				split[i++] = URIUtil.decode(part, "UTF-8");
			} catch (URIException e) {
				throw new RuntimeException(e);
			}
		}
		return split;
	}

	public static String getKey(List<String> parts) {
		ImmutableList.Builder<String> builder = ImmutableList.<String> builder();
		for (String part : parts) {
			try {
				builder.add(URIUtil.encodePath(part));
			} catch (URIException e) {
				throw new RuntimeException(e);
			}
		}
		return Joiner.on(PATH_SEPARATOR).join(builder.build());
	}
}